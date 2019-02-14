package com.yq.exercises.task.decompose;

import java.io.Serializable;

public class TheTask<T, K> implements Serializable {

    public T t;

    public K k;

    public T getT() {
        return t;
    }

    public K getK() {
        return k;
    }


}
