import streamlit as st
import pandas as pd

from versions.v1 import PreprocessV1
from versions.v2 import PreprocessV2
from versions.v3 import PreprocessV3
from versions.v4 import PreprocessV4
from versions.v5 import PreprocessV5
from versions.v6 import PreprocessV6
from versions.v7 import PreprocessV7
from versions.v8 import PreprocessV8
# from versions.v9 import PreprocessV9
# from versions.v10 import PreprocessV10
from versions.v11 import PreprocessV11

from utils.excel import to_excel_with_format


# =========================
# 기본 설정
# =========================
st.set_page_config(page_title="손익분석summary", layout="centered")  #layout="wide"은 풀화면
st.title("📊 손익분석summary")
st.markdown("""
<style>
div[data-baseweb="tab-list"] {
    gap: 35px;   /* ← 숫자 키우면 간격 더 벌어짐 */
}
</style>
""", unsafe_allow_html=True)
# 🔥 여기만 추가
tab1, tab2, tab3 = st.tabs(["매출", "UE", "summary"])

@st.cache_data
def load_excel(file):
    return pd.read_excel(file)


with tab1:

    # =========================
    # session_state 초기화
    # =========================
    if "processed_results" not in st.session_state:
        st.session_state.processed_results = {}

    processed_results = st.session_state.processed_results


    # =========================
    # 1️⃣ 기준 데이터 업로드
    # =========================
    st.header("1️⃣ 손익분석 데이터 업로드")

    base_file = st.file_uploader(
        "기준 엑셀 업로드",
        type=["xlsx"],
        key="base"
    )

    base_df = None

    if base_file:
        base_df = load_excel(base_file)

        # 필수 컬럼 방어
        required_cols = ["상품ID", "판매일자"]
        missing = set(required_cols) - set(base_df.columns)
        if missing:
            st.error(f"기준 데이터에 필수 컬럼이 없습니다: {missing}")
            st.stop()

        base_df["판매일자"] = pd.to_datetime(base_df["판매일자"])
        base_df["판매연도"] = base_df["판매일자"].dt.year
        base_df["판매월"] = base_df["판매일자"].dt.month

        st.success("기준 데이터 업로드 완료")
        st.dataframe(base_df.head(10))


    # =========================
    # 2️⃣ 자동 전처리 영역
    # =========================
    st.header("2️⃣ 상품매출, 수입수수료")

    PROCESSOR_RULES = [
        ("상품매출", PreprocessV1()),
        ("원상회복비", PreprocessV2()),
        ("기타수수료", PreprocessV3()),
        ("매도비", PreprocessV4()),
        ("낙찰수수료", PreprocessV5()),
        ("위탁판매수수료", PreprocessV6()),
        ("상품화", PreprocessV7()),
        ("평가사수수료", PreprocessV8()),
    ]

    uploaded_files = st.file_uploader(
        "엑셀 파일들을 한 번에 업로드하세요",
        type=["xlsx"],
        accept_multiple_files=True
    )

    if base_df is not None and uploaded_files:

        for file in uploaded_files:

            file_name = file.name
            st.subheader(f"📄 {file_name}")

            matched_processor = None
            for keyword, processor in PROCESSOR_RULES:
                if keyword in file_name:
                    matched_processor = processor
                    break

            if matched_processor is None:
                st.warning("⚠️ 파일명으로 처리 유형을 판단할 수 없습니다")
                continue

            df = load_excel(file)

            if not matched_processor.validate(df):
                st.error("❌ 엑셀 구조가 맞지 않습니다")
                continue

            try:
                result_df = matched_processor.preprocess(df, base_df)
            except Exception as e:
                st.error(f"처리 중 오류: {e}")
                continue

            st.success(f"✅ {matched_processor.name} 처리 완료")

            # 상품ID 요약
            if "상품ID" in result_df.columns:
                total = len(result_df)
                null_cnt = result_df["상품ID"].isna().sum()
                dup_cnt = result_df["상품ID"].duplicated().sum()
                valid_cnt = total - null_cnt

                st.markdown(
                    f"""
                    <div style="padding:8px;background:#F5F7FA;border-radius:6px">
                    ✅ 정상 {valid_cnt:,}건 ｜ ⚠️ 빈값 {null_cnt:,}건 ｜ 🔁 중복 {dup_cnt:,}건
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            st.dataframe(result_df.head(20))

            # session 저장
            st.session_state.processed_results[matched_processor.name] = {
                "df": result_df,
                "merge_key": matched_processor.merge_key
            }

            # 다운로드
            st.download_button(
                label=f"⬇ {matched_processor.name} 결과 다운로드",
                data=to_excel_with_format(
                    result_df,
                    highlight_after_col="관리항목2"
                ),
                file_name=f"{matched_processor.name}_처리본.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


    # =========================
    # 3️⃣ v11 통합 집계 (독립 영역)
    # =========================
    st.header("3️⃣ 기타매출 집계")

    v11 = PreprocessV11()

    v11_files = st.file_uploader(
        "기타매출 집계용 파일 업로드",
        type=["xlsx"],
        accept_multiple_files=True,
        key="v11"
    )

if v11_files and base_df is not None:

    transformed_list = []

    for file in v11_files:

        df = load_excel(file)

        if not v11.validate(df):
            st.warning(f"{file.name} 구조 불일치")
            continue

        transformed = v11.preprocess(df, base_df)
        transformed_list.append(transformed)

    if transformed_list:

        final_v11 = pd.concat(transformed_list, ignore_index=True)

        st.success("✅ 기타매출 집계 완료")
        st.dataframe(final_v11.head(20))

        st.download_button(
            "⬇ 기타매출 결과 다운로드",
            data=to_excel_with_format(final_v11),
            file_name="매출_기타매출_통합.xlsx"
        )


    # =========================
    # 4️⃣ 최종 매출 파일
    # =========================
    st.header("4️⃣ 최종 매출 파일")

    if base_df is not None and st.session_state.processed_results:

        if st.button("▶ 최종 머지 실행"):

            final_df = base_df.copy()

            for item in st.session_state.processed_results.values():
                if item["merge_key"]:
                    final_df = final_df.merge(
                        item["df"],
                        on=item["merge_key"],
                        how="left"
                    )

            st.success("🎉 최종 머지 완료")
            st.dataframe(final_df.head(20))

            st.download_button(
                "⬇ 최종 머지 결과 다운로드",
                data=to_excel_with_format(
                    final_df,
                    highlight_after_col="관리항목2"
                ),
                file_name="최종_머지_결과.xlsx"
            )


# UE
with tab2:
    st.info("아직 준비중이다")

# 🔥 summary
with tab3:
    st.info("아직 준비중이다")
