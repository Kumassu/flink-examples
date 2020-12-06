package org.example.flink.model;

public class Person {
    public String name;
    public int age;
    public Gender gender;

    public Person() {
    }

    public Person(String id, int age, Gender gender) {
        this.name = id;
        this.age = age;
        this.gender = gender;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", gender=" + gender +
                '}';
    }
}