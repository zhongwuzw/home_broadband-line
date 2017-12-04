package com.chinamobile.iphelper;
//二分法查找list中小于等于a的最大值的索引

import java.util.List;

public class DichotomySearch {
	public static int search(int[] arr, int key) {
		int l = 0;
		int r = arr.length - 1;
		if (key < arr[l] || key > arr[r]) {
			return -1;
		}
		int m = (l + r) / 2;

		while (l < r) {
			if (arr[m] <= key) {
				l = m;
				if (arr[m] == key || arr[m + 1] > key) {
					return m;
				}
			} else {
				r = m;
				if (arr[m - 1] < key) {
					return m - 1;
				}
			}
			m = (l + r) / 2;
		}
		return -1;
	}

	public static int search_list(List<Long> arr, long key) {
		int l = 0;
		int r = arr.size() - 1;
		if (key < arr.get(l) || key > arr.get(r)) {
			return -1;
		}
		int m = (l + r) / 2;

		while (l < r) {
			if (arr.get(m) <= key) {
				l = m;
				if (arr.get(m) == key || arr.get(m + 1) > key) {
					return m;
				}
			} else {
				r = m;
				if (arr.get(m - 1) < key) {
					return m - 1;
				}
			}
			m = (l + r) / 2;
		}
		return -1;
	}
}