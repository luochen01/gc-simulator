class Solution {
    public int[] processQueries(int[] queries, int m) {
        int[] p = new int[m];
        for (int i = 0; i < m; i++) {
            p[i] = i + 1;
        }

        int[] result = new int[queries.length];
        for (int i = 0; i < queries.length; i++) {
            int index = find(p, queries[i]);
            result[i] = index;
            for (int j = index; j > 0; j--) {
                p[j] = p[j - 1];
            }
            p[0] = queries[i];
        }
        return result;
    }

    private int find(int[] p, int key) {
        for (int i = 0; i < p.length; i++) {
            if (p[i] == key) {
                return i;
            }
        }
        return -1;
    }

    public static void main(String[] args) {
        new Solution().processQueries(new int[] { 3, 1, 2, 1 }, 5);
    }
}