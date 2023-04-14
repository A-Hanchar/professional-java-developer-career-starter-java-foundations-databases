package com.artsiomhanchar.peopledb.annotation;


import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface SQL {
    String value();




//    int age() default 30;
//    String name() default "Frank";
}
