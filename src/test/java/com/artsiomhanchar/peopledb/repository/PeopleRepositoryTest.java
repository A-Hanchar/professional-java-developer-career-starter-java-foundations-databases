package com.artsiomhanchar.peopledb.repository;

import com.artsiomhanchar.peopledb.model.Person;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

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
}
