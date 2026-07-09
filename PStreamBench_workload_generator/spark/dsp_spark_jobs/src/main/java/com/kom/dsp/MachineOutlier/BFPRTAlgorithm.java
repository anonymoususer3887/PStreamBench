package com.kom.dsp.MachineOutlier;

public class BFPRTAlgorithm {

    public static MachineUsage bfprt(MachineUsage[] arr, int left, int right, int k) {
        if (left == right) {
            return arr[left];
        }
        int pivotIndex = partition(arr, left, right);
        int pivotPosition = pivotIndex - left + 1;
        if (k == pivotPosition) {
            return arr[pivotIndex];
        } else if (k < pivotPosition) {
            return bfprt(arr, left, pivotIndex - 1, k);
        } else {
            return bfprt(arr, pivotIndex + 1, right, k - pivotPosition);
        }
    }

    private static int partition(MachineUsage[] arr, int left, int right) {
        MachineUsage pivot = selectPivot(arr, left, right);
        int pivotIndex = left;
        for (int i = left; i < right; i++) {
            if (arr[i].getEuclideanDistance(pivot) < 0) {
                swap(arr, i, pivotIndex);
                pivotIndex++;
            }
        }
        swap(arr, right, pivotIndex);
        return pivotIndex;
    }

    private static MachineUsage selectPivot(MachineUsage[] arr, int left, int right) {
        for (int i = left; i <= right; i += 5) {
            int subRight = Math.min(i + 4, right);
            insertionSort(arr, i, subRight);
            int medianIndex = i + (subRight - i) / 2;
            swap(arr, medianIndex, left + (i - left) / 5);
        }
        int numMedians = (right - left + 1) / 5;
        if ((right - left + 1) % 5 != 0) {
            numMedians++;
        }
        if (numMedians == 1) {
            return arr[left];
        } else {
            return bfprt(arr, left, left + numMedians - 1, numMedians / 2);
        }
    }

    private static void insertionSort(MachineUsage[] arr, int left, int right) {
        for (int i = left + 1; i <= right; i++) {
            MachineUsage key = arr[i];
            int j = i - 1;
            while (j >= left && compare(arr[j], key) > 0) {
                arr[j + 1] = arr[j];
                j--;
            }
            arr[j + 1] = key;
        }
    }

    private static void swap(MachineUsage[] arr, int i, int j) {
        MachineUsage temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    private static int compare(MachineUsage a, MachineUsage b) {
        if (a.getTimestamp() < b.getTimestamp()) {
            return -1;
        } else if (a.getTimestamp() > b.getTimestamp()) {
            return 1;
        } else {
            return a.getMachineId().compareTo(b.getMachineId());
        }
    }
}
