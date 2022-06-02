import seaborn as sns
import matplotlib.pyplot as plt

fig = plt.figure(figsize=[6, 6])

x = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
y = [0.202, 0.1887, 0.1773, 0.17042, 0.17012, 0.168, 0.1641, 0.1646, 0.1672]
time = [3.025, 2.675, 3.372, 2.980, 2.698, 3.460, 3.550, 2.880, 4.291]

ax1 = fig.add_subplot(111)
ax2 = ax1.twinx()

sns.barplot(x, y, ax=ax1, palette='coolwarm_r', color='blue', label="rmse")
sns.pointplot(x, time, ax=ax2, color='red', label='time')

ax1.set_xlabel('feature fraction')
ax1.set_ylabel('RMSE', size=15)
ax2.set_ylabel('time', size=15)
plt.legend(['rmse', 'time'], loc=9)
# ax1.legend(loc=9, bbox_to_anchor=(0.3, 1))
# ax2.legend(['time'], loc=9)
plt.show()
