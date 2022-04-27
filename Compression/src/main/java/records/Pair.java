package records;


public class Pair<K,V>{
    K f0;
    V f1;

    public Pair(K f0, V f1) {
        this.f0 = f0;
        this.f1 = f1;
    }

    public K getF0() {
        return f0;
    }

    public void setF0(K f0) {
        this.f0 = f0;
    }

    public V getF1() {
        return f1;
    }

    public void setF1(V f1) {
        this.f1 = f1;
    }
}
