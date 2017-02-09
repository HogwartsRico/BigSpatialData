package edu.jxust.Common;

public class Hilbert2 {
	// Hilbert填充曲线的排列顺序，实现了空间上邻近排序的要求
	int[][] grid; // 二维网格数组
	int rank; // 阶数

	public Hilbert2(int n) // 构造函数
	{
		rank = n;
		int gridCount=(int) Math.pow(2, n);
		grid = new int[gridCount][gridCount]; // 根据阶数生成网格数
	}

	void hilbertCurve() // 生成Hilbert填充曲线
	{
		for (int i = 1; i <= rank; i++) {
			if (i == 1) {
				grid[0][0] = 0;
				grid[0][1] = 3;
				grid[1][0] = 1;
				grid[1][1] = 2;
			} else {
				for (int j = 0; j < (int) Math.pow(2, (i - 1)); j++)
					for (int k = 0; k < (int) Math.pow(2, (i - 1)); k++) {
						grid[j + (int) Math.pow(2, (i - 1))][k] = (int) (grid[j][k] + Math.pow(4, (i - 1))); // 第1象限对应的网格
						grid[j + (int) Math.pow(2, (i - 1))][k
								+ (int) Math.pow(2, (i - 1))] = (int) (grid[j][k] + 2 * Math.pow(4, (i - 1))); // 第2象限对应的网格
						grid[(int) (Math.pow(2, (i - 1)) - 1) - k][(int) (Math.pow(2, i) - 1)
								- j] = (int) (grid[j][k] + 3 * Math.pow(4, (i - 1)));// 第三象限对应的网格
					}
				for (int j = 0; j < (int) Math.pow(2, (i - 1)); j++) // 第0象限对应的网格
					for (int k = 0; k <= j; k++) {
						int temp = grid[j][k];
						grid[j][k] = grid[k][j]; // 按斜对角线翻转
						grid[k][j] = temp;
					}
			}
		}

	}

	public int hilbertCurve(int x, int y) // 输入数组坐标（x,y）,返回Hilbert排序的序号
	{
		hilbertCurve();
		int value = grid[y][x];
		return value;
	}

	int[][] getHilbertGrid() {
		return grid;
	}

	public int[] hilbertDecoding(int code) // 输入Hilbert序号，返回数组坐标（x,y）
	{
		int[] decode = new int[2];
		for (int i = 0; i < (int) Math.pow(2, rank); i++)
			for (int j = 0; j < (int) Math.pow(2, rank); j++) {
				if (grid[i][j] != code)
					continue;
				else {
					decode[0] = j;
					decode[1] = i;
					break;
				}
			}
		return decode;
	}
}
