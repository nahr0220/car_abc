import pandas as pd
import numpy as np
from .base import BasePreprocessor


class PreprocessV11(BasePreprocessor):
    name = "v11(기타매출집계)"
    merge_key = "상품ID"

    required_columns = ['계정코드', '계정명', '회계일자', 'NO', '적요','거래처코드', '거래처', '차변', '대변', '작성사원명']

    keep_columns = required_columns.copy()

    def validate(self, df):
        return set(self.required_columns).issubset(df.columns)

    def preprocess(self, df_11, df):

        df_11 = df_11[self.keep_columns].copy()
        df_11 = df_11[~df_11['회계일자'].isin(['월계', '누계'])]

        df_11['회계일자'] = pd.to_datetime(df_11['회계일자'])
        df_11['회계연도'] = df_11['회계일자'].dt.year
        df_11['회계월'] = df_11['회계일자'].dt.month
        df_11['회계일자'] = df_11['회계일자'].dt.date

        # -----------------------------
        # 2. 차량번호 추출
        # -----------------------------
        unit_pattern = (
            r'(?:'
            r'(?:서울|부산|대구|인천|광주|대전|울산|경기)?'
            r'\d{2,3}[가-힣]\s?\d{4}'   # ← 여기 수정
            r'|지게차'
            r')'
        )

        df_11['차량번호'] = df_11['적요'].str.findall(unit_pattern).str[0]

        def get_product_id(row):
            ref = df[
                (df['판매연도'] == row['회계연도']) &
                (df['판매월'] == row['회계월'])
            ]

            if ref.empty or pd.isna(row['차량번호']):
                return None

            for col in ['신차량번호', '구차량번호']:
                match = ref[ref[col] == row['차량번호']]
                if not match.empty:
                    return match['상품ID'].iloc[0]

            return None


        df_11['상품ID'] = df_11.apply(get_product_id, axis=1)

        # 조건 마스크 생성
        mask_flos_care = df_11['계정명'].isin(['기타매출(리본케어플러스)', '기타매출(리본케어)'])
        mask_etc = df_11['계정명'] == '기타매출(엔카홈서비스)'
        mask_delivery = df_11['계정명'] == '기타매출(탁송비)'

        # 기본값 빈값으로 초기화
        df_11['비고'] = ''

        # 1️⃣ 리본케어 / 리본케어플러스
        df_11.loc[mask_flos_care, '비고'] = np.where(
            df_11.loc[mask_flos_care, '적요'].str.contains('매출취소|환불', na=False),
            '매출취소',
            ''
        )

        # 2️⃣ 기타
        df_11.loc[mask_etc, '비고'] = np.where(
            df_11.loc[mask_etc, '적요'].str.contains('홈서비스|엔카믿고', na=False),
            '홈서비스',
            '확인필요'
        )

        # 3️⃣ 탁송
        df_11.loc[mask_delivery, '비고'] = np.where(
            df_11.loc[mask_delivery, '상품ID'].str.startswith('C', na=False),
            '',
            np.where(
                df_11.loc[mask_delivery, '적요'].str.contains(
                    '판매취소|판매 취소|계약취소|계약 취소|단순변심|엔카믿고',
                    na=False
                ),
                '판매취소',
                '확인필요'
            )
        )    
        return df_11
