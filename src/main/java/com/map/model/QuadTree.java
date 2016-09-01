package com.map.model;

import java.util.ArrayList;
import java.util.List;

public class QuadTree<T> {

    private Node<T> root;

    public static class Node<T> {

        public double x, y;              // x- and y- coordinates
        public Node<T> NW, NE, SE, SW;            // four subtrees
        public T s;                // associated data

        Node(double x, double y, T s) {
            this.x = x;
            this.y = y;
            this.s = s;
        }

        @Override
        public String toString() {
            return s + "( " + x + "," + y + " )";
        }
    }

    public static class Box {

        double xlow, ylow, xhigh, yhigh;

        public Box(double xlow, double ylow, double xhigh, double yhigh) {
            this.xlow = xlow;
            this.xhigh = xhigh;
            this.ylow = ylow;
            this.yhigh = yhigh;
        }

        public boolean contains(double x, double y) {
            return (x >= xlow && y >= ylow && x <= xhigh && y <= yhigh);
        }

        @Override
        public String toString() {
            return "( " + xlow + ", " + ylow + ", " + xhigh + ", " + yhigh + " )";
        }
    }


    public void insert(double x, double y, T s) {
        root = insert(root, x, y, s);
    }

    private Node<T> insert(Node<T> h, double x, double y, T s) {
        if (h == null)
            return new Node<T>(x, y, s);

        else if (x < h.x && y < h.y)
            h.SW = insert(h.SW, x, y, s);
        else if (x < h.x && !(y < h.y))
            h.NW = insert(h.NW, x, y, s);
        else if (!(x < h.x) && y < h.y)
            h.SE = insert(h.SE, x, y, s);
        else if (!(x < h.x) && !(y < h.y))
            h.NE = insert(h.NE, x, y, s);
        return h;
    }

    public List<Node<T>> query(Box box) {
        List<Node<T>> result = new ArrayList<Node<T>>();
        query(root, box, result);
        return result;
    }

    private void query(Node<T> h, Box box, List<Node<T>> result) {
        if (h == null)
            return;

        if (box.contains(h.x, h.y))
            result.add(h);

        if ( (box.xlow < h.x) &&  (box.ylow < h.y))
            query(h.SW, box, result);
        if ( (box.xlow < h.x) && !(box.yhigh < h.y))
            query(h.NW, box, result);
        if (!(box.xhigh < h.x) &&  (box.ylow < h.y))
            query(h.SE, box, result);
        if (!(box.xhigh < h.x) && !(box.yhigh < h.y))
            query(h.NE, box, result);
    }

}
