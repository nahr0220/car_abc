import pandas as pd
import numpy as np
from .base import BasePreprocessor

class PreprocessV3(BasePreprocessor):
    name = "v3(기타수수료)"
    merge_key = None  # 머지 안 함 (분류용)

    def validate(self, df):
        return {"회계일자", "적요", "관리항목2"}.issubset(df.columns)

    def preprocess(self, df_3, df=None):
        end_idx = df_3.columns.get_loc("관리항목2")
        df_3 = df_3.iloc[:, :end_idx + 1]

        df_3 = df_3[~df_3['회계일자'].isin(['월계', '누계'])]
        df_3['회계일자'] = pd.to_datetime(df_3['회계일자'])
        df_3['회계연도'] = df_3['회계일자'].dt.year
        df_3['회계월'] = df_3['회계일자'].dt.month
        df_3['회계일자'] = df_3['회계일자'].dt.date

        conditions = [
            df_3['적요'].str.contains("보관료|운반비|탁송료 환불", na=False),
            df_3['적요'].str.contains("연회비", na=False),
            df_3['적요'].str.contains("낙찰취소 위약금", na=False),
            df_3['적요'].str.contains("성능책임보험|성능점검인협동조합", na=False),
            df_3['적요'].str.contains("잡이익", na=False),
            df_3['적요'].str.contains("신차구매 수수료|신차구매수수료", na=False),
            df_3['적요'].str.contains("KB국민카드", na=False),
            df_3['적요'].str.contains("계약금|계약취소|수출 환불금|수출 취소", na=False),
            df_3['적요'].str.contains("용역료|인력지원", na=False),
            df_3['적요'].str.contains("인센티브", na=False),
            df_3['적요'].str.contains("PGM|캐롯|TM 수수료|리스|금융수수료", na=False)
        ]

        choices = [
            "보관료", "차옥션연회비", "낙찰취소 위약금", "데이터지급수수료",
            "잡이익", "신차구매수수료", "카드수수료", "계약금",
            "용역료", "인센티브", "금융수수료"
        ]

        df_3['구분'] = np.select(conditions, choices, default="")
        df_3['배부'] = np.where(df_3['구분'] == '차옥션연회비', '연회비', '연회비 외')
        
        return df_3
