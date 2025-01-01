package com.dsp;

public class Calc {

    public static double calcP(int n1, int n2, int n3, int c0, int c1, int c2) {
        double k2 = calcKi(n2);
        double k3 = calcKi(n3);
        double first = k3*(n3/c2);
        double second = (1-k3)*k2*(n2/c1);
        double third = (1-k3)*(1-k2)*(n1/c0);
        return first + second + third;
    }

    
    public static double calcKi(int ni) {
        double log_ni = Math.log(ni+1);
        double numerator = log_ni + 1;
        double denominator = log_ni + 2;
        return numerator/denominator;
    }
}
