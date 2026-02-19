import pandas as pd
import numpy as np
from .base import BasePreprocessor

class PreprocessV7(BasePreprocessor):
    name = "v7(상품화)"
    merge_key = "상품ID"

    def validate(self, df):
        return {"회계일자", "적요", "대변", "관리항목2"}.issubset(df.columns)

    def preprocess(self, df_7, df):
        end_idx = df_7.columns.get_loc("관리항목2")
        df_7 = df_7.iloc[:, :end_idx + 1]

        df_7 = df_7[~df_7['회계일자'].isin(['월계', '누계'])]
        df_7['회계일자'] = pd.to_datetime(df_7['회계일자'])
        df_7['회계연도'] = df_7['회계일자'].dt.year
        df_7['회계월'] = df_7['회계일자'].dt.month
        df_7['회계일자'] = df_7['회계일자'].dt.date

        unit_pattern = (
            r'(?:'
            r'(?:서울|부산|대구|인천|광주|대전|울산|경기)?\d{2,3}[가-힣]\d{4}'
            r'|지게차)'
        )

        df_7['차량번호'] = df_7['적요'].str.findall(unit_pattern).str[0]

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

        df_7['상품ID'] = df_7.apply(get_product_id, axis=1)
        mapping = {
            '디비손해보험주식회사': '디비손해보험',
            '디비손해보험 주식회사': '디비손해보험',
            '(주)레드캡투어': '레드캡투어',
            '롯데렌탈(주)': '롯데렌탈',
            '롯데캐피탈': '롯데캐피탈',
            '삼성카드주식회사': '삼성카드',
            '(주)마더브레인': '삼성화재',
            '신한마이카': '신한마이카',
            '(주)신한은행송현동금융센터': '신한마이카',
            '주식회사쏘카': '쏘카',
            '엔에이치농협캐피탈주식회사': '엔에이치농협캐피탈',
            '엠지캐피탈(주)': '엠지캐피탈',
            'MG캐피탈': '엠지캐피탈',
            '오릭스캐피탈코리아 주식회사': '오릭스캐피탈',
            '오토플러스(주)': '오토플러스',
            '우리금융캐피탈 주식회사': '우리금융캐피탈',
            '우리금융캐피탈': '우리금융캐피탈',
            '우리금융캐피탈주식회사': '우리금융캐피탈',
            '주식회사 하나애드아이엠씨': '하나애드아이엠씨',
            '하나캐피탈': '하나캐피탈',
            '하나캐피탈(주)': '하나캐피탈',
            '현대글로비스 주식회사': '현대글로비스',
            '현대자동차(주)양산중고차센터': '현대자동차',
            '현대자동차(주)용인중고차센터': '현대자동차',
            '현대캐피탈 주식회사': '현대캐피탈',
            '현대캐피탈': '현대캐피탈'
        }
        df_7['거래처2'] = df_7['거래처'].map(mapping).fillna(df_7['거래처'])
        df_7['비고'] = np.where((df['차량번호'].isna()) | (df['적요'].str.contains('외 ', na=False)),'확인필요','')

        return df_7
