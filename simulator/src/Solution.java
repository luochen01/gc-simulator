import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

class Point {
    final int i;
    final int j;

    public Point(int i, int j) {
        this.i = i;
        this.j = j;
    }

    @Override
    public boolean equals(Object o) {
        Point p = (Point) o;
        return i == p.i && j == p.j;
    }

    @Override
    public int hashCode() {
        return i * 31 + j;
    }
}

class UnionFind {
    private final Map<Point, Point> map = new HashMap<>();

    public Point find(Point p) {
        Point parent = map.get(p);
        if (parent != null) {
            Point root = find(parent);
            map.put(p, root);
            return root;
        } else {
            return p;
        }
    }

    public void union(Point p1, Point p2) {
        Point r1 = find(p1);
        Point r2 = find(p2);
        if (!r1.equals(r2)) {
            map.put(r1, r2);
        }
    }

}

class Solution {
    public int swimInWater(int[][] grid) {
        int m = grid.length;
        int n = grid[0].length;

        boolean[][] visited = new boolean[m][n];

        PriorityQueue<Point> pq = new PriorityQueue<>((p1, p2) -> Integer.compare(grid[p1.i][p1.j], grid[p2.i][p2.j]));
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                pq.add(new Point(i, j));
            }
        }

        UnionFind uf = new UnionFind();
        Point src = new Point(0, 0);
        Point dest = new Point(m - 1, n - 1);
        while (!pq.isEmpty()) {
            Point p = pq.poll();
            visited[p.i][p.j] = true;

            connect(uf, visited, p, p.i + 1, p.j);
            connect(uf, visited, p, p.i - 1, p.j);
            connect(uf, visited, p, p.i, p.j - 1);
            connect(uf, visited, p, p.i, p.j + 1);

            if (uf.find(src).equals(uf.find(dest))) {
                return grid[p.i][p.j];
            }
        }

        return -1;
    }

    private void connect(UnionFind uf, boolean[][] visited, Point p, int i, int j) {
        if (i >= 0 && i < visited.length && j >= 0 && j < visited[0].length && visited[i][j]) {
            uf.union(p, new Point(i, j));
        }
    }

    public static void main(String[] args) {
        new Solution().swimInWater(new int[][] { { 1, 2 }, { 1, 3 } });
    }
}