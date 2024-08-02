package cn.kgc.forDemo;

import java.util.ArrayList;
import java.util.List;

public class ForDemo {
    public static void main(String[] args) {
        List<String> strings = new ArrayList<String>();
        strings.add("111");
        strings.add("222");
        for (String s :
                strings) {
            System.out.println(s);
        }

        int[] ints = {1, 2, 3, 4};
        for (int i :
                ints) {
            System.out.println(i);
        }
    }
}
