class Solution {
    private static final int MOD = (int) (1e9 + 7);

    public int findGoodStrings(int n, String s1, String s2, String evil) {
        long[][] dp = new long[n][26];
        char e = evil.charAt(evil.length() - 1);

        for (int i = 0; i < n; i++) {
            for (char c = 'a'; c <= 'z'; c++) {
                if (c >= s1.charAt(i) && c <= s2.charAt(i)) {
                    dp[i][c - 'a'] = 1;
                }
                for (int j = 0; j < i; j++) {
                    // check previous strings
                    char low = s1.charAt(j);
                    char high = s2.charAt(j);
                    // check all chars between low and high
                    long sum = 0;
                    for (char tmp = (char) (low + 1); tmp <= high - 1; tmp++) {
                        sum += dp[i][tmp - 'a'];
                    }
                    dp[i][c - 'a'] = (dp[i][c - 'a'] + sum * (long) Math.pow(26, i - j - 1)) % MOD;
                }
                if (c == e && i + 1 >= evil.length()) {
                    long sum = 0;
                    if (i + 1 == evil.length()) {
                        sum = 1;
                    } else {
                        for (int j = 0; j < 26; j++) {
                            sum = (sum + dp[i - evil.length()][j]) % MOD;
                        }
                    }
                    dp[i][c - 'a'] = (dp[i][c - 'a'] + MOD - sum) % MOD;
                }
            }
        }
        long sum = 0;
        for (int i = 0; i < 26; i++) {
            sum += dp[n - 1][i];
        }
        return (int) (sum % MOD);

    }

    public static void main(String[] args) {
        new Solution().findGoodStrings(2, "aa", "da", "b");
    }
}