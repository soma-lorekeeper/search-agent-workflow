from src.agent import ask
from src.logging_config import setup_logging

setup_logging()

QUERIES = [
    ("Q1", "1화에서 김독자가 첫 시나리오를 돌파한 방법이 뭐였는지, 그리고 비슷한 방식(회유/우회)을 이후 화에서도 다시 쓴 적이 있어?"),
    ("Q2", "'부러진 신념'이라는 아이템 어떻게 손에 넣었고, 최종적으로 어떻게 됐어?"),
    ("Q3", "한명오가 유상아의 물건을 몰래 가져간 적이 있어?"),
    ("Q4", "이현성의 능력치(체력/근력/민첩/마력)가 화가 지날수록 어떻게 바뀌었어?"),
    ("Q5", "천인호는 몇 화에서 죽어? 누구 손에?"),
    ("Q7", "김독자가 유중혁과 정식으로 계약을 맺은 적이 있어?"),
]

Q6_TURN1 = "정희원이라는 캐릭터 소개해줘"
Q6_TURN2 = "이 캐릭터가 예전에 어떤 물건을 남한테 나눠준 적 있어?"


def main() -> None:
    for name, question in QUERIES:
        print(f"===== {name} =====")
        print("Q:", question)
        answer = ask(question, thread_id=name)
        print("A:", answer)
        print()

    print("===== Q6 (multi-turn) =====")
    print("Turn1 Q:", Q6_TURN1)
    a1 = ask(Q6_TURN1, thread_id="Q6")
    print("Turn1 A:", a1)
    print("Turn2 Q:", Q6_TURN2)
    a2 = ask(Q6_TURN2, thread_id="Q6")
    print("Turn2 A:", a2)


if __name__ == "__main__":
    main()
