package com.dsp;

public class Calc {
    public double calcP(double k3, double k2, double n3, double n2, double n1, double c2, double c1, double c0) {
        double first = k3*(n3/c2);
        double second = (1-k3)*k2*(n2/c1);
        double third = (1-k3)*(1-k2)*(n1/c0);
        return first + second + third;
    }

    
    public double calcKi(int ni) {
        double log_ni = Math.log(ni+1);
        double numerator = log_ni + 1;
        double denominator = log_ni + 2;
        return numerator/denominator;
    }
}
