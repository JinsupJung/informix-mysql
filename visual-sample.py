import matplotlib.pyplot as plt
import numpy as np

# 데이터 설정
months = ['2024-01', '2024-02', '2024-03']
previous_arrears = [500000, 900000, 1200000]  # 전월미수금
sales = [1200000, 1500000, 1800000]  # 이번달 매출
collections = [800000, 1300000, 1600000]  # 이번달 수금
current_arrears = [900000, 1200000, 1400000]  # 현재 미수금

x = np.arange(len(months))  # 월 데이터에 대한 위치
width = 0.2  # 막대 너비

fig, ax = plt.subplots()
rects1 = ax.bar(x - 3*width/2, previous_arrears, width, label='Previous Arrears')
rects2 = ax.bar(x - width/2, sales, width, label='Sales')
rects3 = ax.bar(x + width/2, collections, width, label='Collections')
rects4 = ax.bar(x + 3*width/2, current_arrears, width, label='Current Arrears')

# 레이블, 타이틀 및 커스텀 x축 틱 레이블 추가
ax.set_xlabel('Month')
ax.set_ylabel('Amount')
ax.set_title('Financial Overview by Month')
ax.set_xticks(x)
ax.set_xticklabels(months)
ax.legend()

# 막대 레이블 추가 함수
def autolabel(rects):
    """각 막대 위에 값을 표시"""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

# 각 막대에 레이블 추가
autolabel(rects1)
autolabel(rects2)
autolabel(rects3)
autolabel(rects4)

fig.tight_layout()

plt.show()
