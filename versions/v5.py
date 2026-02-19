import pandas as pd
import numpy as np
from .base import BasePreprocessor

class PreprocessV5(BasePreprocessor):
    name = "v5(낙찰수수료)"
    merge_key = "상품ID"

    def validate(self, df):
        required = {"회계일자", "적요", "대변", "관리항목2"}
        return required.issubset(df.columns)

    def preprocess(self, df_5, base_df):
        # 컬럼 컷
        end_idx = df_5.columns.get_loc("관리항목2")
        df_5 = df_5.iloc[:, :end_idx + 1]

        # 날짜 처리
        df_5 = df_5[~df_5['회계일자'].isin(['월계', '누계'])]
        df_5['회계일자'] = pd.to_datetime(df_5['회계일자'])
        df_5['회계연도'] = df_5['회계일자'].dt.year
        df_5['회계월'] = df_5['회계일자'].dt.month
        df_5['회계일자'] = df_5['회계일자'].dt.date

        # 차량번호 추출
        unit_pattern = (
            r'(?:'
            r'(?:서울|부산|대구|인천|광주|대전|울산|경기)?\d{2,3}[가-힣]\d{4}'
            r'|지게차)'
        )
        df_5['차량번호'] = df_5['적요'].str.findall(unit_pattern).str[0]

        # 상품ID 매칭
        def get_product_id(row):
            ref = base_df[
                (base_df['판매연도'] == row['회계연도']) &
                (base_df['판매월'] == row['회계월'])
            ]
            if pd.isna(row['차량번호']):
                return None

            for col in ['신차량번호', '구차량번호']:
                m = ref[ref[col] == row['차량번호']]
                if not m.empty:
                    return m['상품ID'].iloc[0]
            return None

        df_5['상품ID'] = df_5.apply(get_product_id, axis=1)

        # 분류
        conditions = [
            df_5['적요'].str.contains(
                '낙찰취소 수수료|낙찰취소 위약금|낙찰취소수수료|낙찰취소위약금',
                na=False
            ),
            df_5['적요'].str.contains('자산|LC', na=False),
            df_5['적요'].str.contains('외부|위탁', na=False)
        ]
        choices = ['낙찰취소수수료', '자산', '외부출품']
        df_5['분류'] = np.select(conditions, choices, default='낙찰수수료')

        # 판매월 일치 여부
        df_ref = base_df[['상품ID', '판매연도', '판매월']].drop_duplicates('상품ID')
        df_5 = df_5.merge(df_ref, on='상품ID', how='left')

        df_5['판매월일치여부'] = np.where(
            df_5['판매월'].isna(),
            '',
            np.where(df_5['회계월'] == df_5['판매월'], 'TRUE', 'FALSE')
        )

        # 중복 여부
        df_5['중복'] = np.where(
            df_5['상품ID'].notna() & df_5['상품ID'].duplicated(keep=False),
            'TRUE', 'FALSE'
        )

        # 취소 로직
        df_5['abs_v'] = df_5['대변'].abs()
        df_5['seq'] = df_5.groupby(
            ['회계연도', '회계월', '차량번호', 'abs_v', df_5['대변'] > 0]
        ).cumcount()

        canceled = df_5.groupby(
            ['회계연도', '회계월', '차량번호', 'abs_v', 'seq']
        )['대변'].transform('count') > 1

        df_5['비고'] = np.where(canceled, '취소', '')
        df_5.loc[df_5['비고'] == '취소', '상품ID'] = np.nan

        df_5.drop(columns=['abs_v', 'seq'], inplace=True)

        return df_5