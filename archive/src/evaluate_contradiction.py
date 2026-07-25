"""check_claim()의 판정 정확도를, 이미 만들어둔 정답 라벨셋(eval/contradiction_test_set.json)
으로 검증한다. claim 추출 단계는 건너뛰고(정답셋이 이미 claim 단위이므로), 검색+판정 절반만
직접 평가한다."""

import json

from src.config import ROOT_DIR
from src.contradiction_check import check_claim
from src.logging_config import setup_logging

setup_logging()

DATASET_PATH = ROOT_DIR / "eval" / "contradiction_test_set.json"


def main() -> None:
    cases = json.loads(DATASET_PATH.read_text(encoding="utf-8"))
    correct = 0
    mismatches = []

    for case in cases:
        claim = {
            "quote": case["candidate_statement"],
            "entities": [],
            "category": case["category"],
        }
        result = check_claim(claim)
        predicted = result.get("label")
        expected = case["label"]
        match = predicted == expected
        correct += match

        status = "OK" if match else "MISMATCH"
        print(f"{case['id']} [{case['category']}] 기대={expected} 예측={predicted} {status}")
        if not match:
            mismatches.append((case, result))

    print(f"\n정확도: {correct}/{len(cases)}")

    if mismatches:
        print("\n=== 불일치 상세 ===")
        for case, result in mismatches:
            print(f"\n{case['id']}: {case['candidate_statement']}")
            print(f"  정답: {case['label']} ({case['established_fact']}, {case['source_episode']}화)")
            print(
                f"  예측: {result.get('label')} "
                f"({result.get('established_fact')}, {result.get('source_episode')}화)"
            )
            print(f"  예측 근거 설명: {result.get('explanation')}")


if __name__ == "__main__":
    main()
