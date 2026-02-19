import pandas as pd
import numpy as np
from .base import BasePreprocessor

class PreprocessV1(BasePreprocessor):
    name = "v1(상품매출)"
    merge_key = "상품ID"

    def validate(self, df):
        return {"회계일자", "적요", "대변", "관리항목2"}.issubset(df.columns)

    def preprocess(self, df_1, df):
        # 컬럼 컷
        end_idx = df_1.columns.get_loc("관리항목2")
        df_1 = df_1.iloc[:, :end_idx + 1]

        # 날짜 처리
        df_1 = df_1[~df_1['회계일자'].isin(['월계', '누계'])]
        df_1['회계일자'] = pd.to_datetime(df_1['회계일자'])
        df_1['회계연도'] = df_1['회계일자'].dt.year
        df_1['회계월'] = df_1['회계일자'].dt.month
        df_1['회계일자'] = df_1['회계일자'].dt.date

        # 차량번호 추출
        unit_pattern = (
            r'(?:'
            r'(?:서울|부산|대구|인천|광주|대전|울산|경기)?\d{2,3}[가-힣]\d{4}'
            r'|지게차)'
        )

        df_1['차량번호1'] = df_1['적요'].str.extract(rf'({unit_pattern})\(')
        df_1['차량번호2'] = df_1['적요'].str.extract(rf'\(({unit_pattern})')

        # 상품ID 매칭
        def get_product_id(row):
            ref = df[
                (df['판매연도'] == row['회계연도']) &
                (df['판매월'] == row['회계월'])
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

        df_1['상품ID'] = df_1.apply(get_product_id, axis=1)
        df_1['상품ID'] = np.where(
            df_1['차량번호1'] == '지게차',
            df_1['적요'].str[:12],
            df_1['상품ID']
        )

        # 판매처 머지
        df_ref = df[['상품ID', '판매연도', '판매월', '판매처']].drop_duplicates('상품ID')
        df_1 = df_1.merge(df_ref, on='상품ID', how='left')

        df_1['판매월일치여부'] = np.where(
            df_1['판매월'].isna(), 
            '', np.where(pd.to_numeric(df_1['회계월'], errors='coerce') == pd.to_numeric(df_1['판매월'], errors='coerce'),'TRUE', 'FALSE'))

        # 취소 로직
        df_1['abs_v'] = df_1['대변'].abs()
        df_1['seq'] = df_1.groupby(
            ['회계연도', '회계월', '차량번호1', 'abs_v', df_1['대변'] > 0]
        ).cumcount()

        canceled = df_1.groupby(
            ['회계연도', '회계월', '차량번호1', 'abs_v', 'seq']
        )['대변'].transform('count') > 1

        df_1['비고'] = np.where(canceled, '취소', '')
        df_1.loc[df_1['비고'] == '취소', '상품ID'] = np.nan
        df_1.drop(columns=['abs_v', 'seq'], inplace=True)

        return df_1
