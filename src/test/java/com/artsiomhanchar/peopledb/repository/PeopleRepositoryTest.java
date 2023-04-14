package com.artsiomhanchar.peopledb.repository;

import com.artsiomhanchar.peopledb.model.Person;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTest {

    private Connection connection;
    private PeopleRepository repo;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:C:\\Users\\ahanchar\\Desktop\\java\\databases\\peopledb".replace("~", System.getProperty("user.home")));
        connection.setAutoCommit(false);

        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 00, 0, ZoneId.of("-6")));

        Person savedPerson = repo.save(john);

        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPersons() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 00, 0, ZoneId.of("-6")));
        Person bobby = new Person("Bobby", "Smith", ZonedDateTime.of(1982, 9, 13, 13, 13, 00, 0, ZoneId.of("-8")));

        Person savedPerson1 = repo.save(john);
        Person savedPerson2 = repo.save(bobby);

        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canFindPersonById() {
        Person test = new Person("Test", "Johnson", ZonedDateTime.of(2000, 9, 1, 12, 0, 0, 0, ZoneId.of("+0")));

        Person savedPerson = repo.save(test);
        Long savedPersonId = savedPerson.getId();

        Person foundPerson = repo.findById(savedPersonId).get();

        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = repo.findById(-1L);

        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canFindAll() {
        repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John1", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John2", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John3", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John4", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John5", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John6", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John7", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John8", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));

        List<Person> people = repo.findAllPersons();
        assertThat(people.size()).isGreaterThanOrEqualTo(10);
    }

    @Test
    public void canGetCount() {
        long startCount = repo.count();

        repo.save(new Person("John1", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));

        long endCount = repo.count();

        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete() {
        Person savedPerson = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));

        long startCount = repo.count();

        repo.delete(savedPerson);

        long endCount = repo.count();

        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person p1 = repo.save(new Person("John1", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        Person p2 = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));

        long startCount = repo.count();

        repo.delete(p1, p2);

        long endCount = repo.count();

        assertThat(endCount).isEqualTo(startCount - 2);
    }

    @Test
    public void experiment() {
        Person p1 = new Person(10L, null, null, null);
        Person p2 = new Person(20L, null, null, null);
        Person p3 = new Person(30L, null, null, null);
        Person p4 = new Person(40L, null, null, null);
        Person p5 = new Person(50L, null, null, null);

        // DELETE FROM PEOPLE WHERE ID IN (10,20,30,40,50);

        Person[] people = Arrays.asList(p1, p2, p3, p4, p5).toArray(new Person[]{});

        String ids = Arrays.stream(people)
                .map(Person::getId)
                .map(String::valueOf) // 10L => "10"
                .collect(
                        Collectors.joining(",")
                );

        System.out.println(ids);
    }

    @Test
    public void canUpdate() {
        Person savedPerson = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));

        Person p1 = repo.findById(savedPerson.getId()).get();

        savedPerson.setSalary(new BigDecimal("73000.28"));
        repo.update(savedPerson);

        Person p2 = repo.findById(savedPerson.getId()).get();

        assertThat(p2.getSalary()).isNotEqualTo(p1.getSalary());
    }
}
