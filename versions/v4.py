import pandas as pd
import numpy as np
from .base import BasePreprocessor

class PreprocessV4(BasePreprocessor):
    name = "v4(매도비)"
    merge_key = "상품ID"

    def validate(self, df):
        required = {"회계일자", "적요", "대변", "관리항목2"}
        return required.issubset(df.columns)

    def preprocess(self, df_4, base_df):
        # 컬럼 컷
        end_idx = df_4.columns.get_loc("관리항목2")
        df_4 = df_4.iloc[:, :end_idx + 1]

        # 날짜 처리
        df_4 = df_4[~df_4['회계일자'].isin(['월계', '누계'])]
        df_4['회계일자'] = pd.to_datetime(df_4['회계일자'])
        df_4['회계연도'] = df_4['회계일자'].dt.year
        df_4['회계월'] = df_4['회계일자'].dt.month
        df_4['회계일자'] = df_4['회계일자'].dt.date

        # 차량번호 추출
        unit_pattern = (
            r'(?:'
            r'(?:서울|부산|대구|인천|광주|대전|울산|경기)?\d{2,3}[가-힣]\d{4}'
            r'|지게차)'
        )

        df_4['차량번호1'] = df_4['적요'].str.extract(rf'({unit_pattern})\(')
        df_4['차량번호2'] = df_4['적요'].str.extract(rf'\(({unit_pattern})')

        # 상품ID 매칭
        def get_product_id(row):
            ref = base_df[
                (base_df['판매연도'] == row['회계연도']) &
                (base_df['판매월'] == row['회계월'])
            ]
            for c1, c2 in [
                ('차량번호1', '신차량번호'),
                ('차량번호2', '신차량번호'),
                ('차량번호1', '구차량번호'),
                ('차량번호2', '구차량번호')
            ]:
                if pd.notna(row[c1]):
                    m = ref[ref[c2] == row[c1]]
                    if not m.empty:
                        return m['상품ID'].iloc[0]
            return None

        df_4['상품ID'] = df_4.apply(get_product_id, axis=1)

        # 판매월 일치 여부
        df_ref = base_df[['상품ID', '판매연도', '판매월']].drop_duplicates('상품ID')
        df_4 = df_4.merge(df_ref, on='상품ID', how='left')

        df_4['판매월일치여부'] = np.where(
            df_4['판매월'].isna(),
            '',
            np.where(df_4['회계월'] == df_4['판매월'], 'TRUE', 'FALSE')
        )

        # 중복 여부
        df_4['중복'] = np.where(
            df_4['상품ID'].notna() & df_4['상품ID'].duplicated(keep=False),
            'TRUE', 'FALSE'
        )

        # 취소 로직
        df_4['abs_v'] = df_4['대변'].abs()
        df_4['seq'] = df_4.groupby(
            ['회계연도', '회계월', '차량번호1', 'abs_v', df_4['대변'] > 0]
        ).cumcount()

        canceled = df_4.groupby(
            ['회계연도', '회계월', '차량번호1', 'abs_v', 'seq']
        )['대변'].transform('count') > 1

        df_4['비고'] = np.where(canceled, '취소', '')
        df_4.loc[df_4['비고'] == '취소', '상품ID'] = np.nan

        df_4.drop(columns=['abs_v', 'seq'], inplace=True)

        return df_4
