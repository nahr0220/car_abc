import pandas as pd
import numpy as np
from .base import BasePreprocessor

class PreprocessV2(BasePreprocessor):
    name = "v2(원상회복비)"
    merge_key = "상품ID"

    def validate(self, df):
        return {"회계일자", "적요", "대변", "관리항목2"}.issubset(df.columns)

    def preprocess(self, df_2, df):
        end_idx = df_2.columns.get_loc("관리항목2")
        df_2 = df_2.iloc[:, :end_idx + 1]

        df_2 = df_2[~df_2['회계일자'].isin(['월계', '누계'])]
        df_2['회계일자'] = pd.to_datetime(df_2['회계일자'])
        df_2['회계연도'] = df_2['회계일자'].dt.year
        df_2['회계월'] = df_2['회계일자'].dt.month
        df_2['회계일자'] = df_2['회계일자'].dt.date

        unit_pattern = (
            r'(?:'
            r'(?:서울|부산|대구|인천|광주|대전|울산|경기)?\d{2,3}[가-힣]\d{4}'
            r'|지게차)'
        )

        df_2['차량번호'] = df_2['적요'].str.findall(unit_pattern).str[0]

        def get_product_id(row):
            ref = df[
                (df['판매연도'] == row['회계연도']) &
                (df['판매월'] == row['회계월'])
            ]
            for col in ['신차량번호', '구차량번호']:
                m = ref[ref[col] == row['차량번호']]
                if not m.empty:
                    return m['상품ID'].iloc[0]
            return None

        df_2['상품ID'] = df_2.apply(get_product_id, axis=1)

        df_ref = df[['상품ID', '판매연도', '판매월']].drop_duplicates('상품ID')
        df_2 = df_2.merge(df_ref, on='상품ID', how='left')

        df_2['판매월일치여부'] = np.where(
            df_2['판매월'].isna(), 
            '', np.where(pd.to_numeric(df_2['회계월'], errors='coerce') == pd.to_numeric(df_2['판매월'], errors='coerce'),'TRUE', 'FALSE'))


        # 취소 로직
        df_2['abs_v'] = df_2['대변'].abs()
        df_2['seq'] = df_2.groupby(
            ['회계연도', '회계월', '차량번호', 'abs_v', df_2['대변'] > 0]
        ).cumcount()

        canceled = df_2.groupby(
            ['회계연도', '회계월', '차량번호', 'abs_v', 'seq']
        )['대변'].transform('count') > 1

        df_2['비고'] = np.where(canceled, '취소', '')
        df_2.loc[df_2['비고'] == '취소', '상품ID'] = np.nan
        df_2.drop(columns=['abs_v', 'seq'], inplace=True)
        df_2['배부'] = np.where(df_2['판매월일치여부'] == "TRUE", '직접', '간접')

        return df_2
