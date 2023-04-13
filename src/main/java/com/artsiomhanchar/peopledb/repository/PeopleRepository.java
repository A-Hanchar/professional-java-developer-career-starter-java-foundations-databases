package com.artsiomhanchar.peopledb.repository;

import com.artsiomhanchar.peopledb.exeption.UnableToSaveException;
import com.artsiomhanchar.peopledb.model.Person;

import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class PeopleRepository {
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)";
    private Connection connection;

    public PeopleRepository(Connection connection) {
        this.connection = connection;
    }

    public Person save(Person person) throws UnableToSaveException {
//        String sql = String.format(
//                "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES('%s', '%s', '%s')",
//                person.getFirstName(), person.getLastName(), person.getDob()
//        );

        try {
            PreparedStatement ps = connection.prepareStatement(SAVE_PERSON_SQL, Statement.RETURN_GENERATED_KEYS);

            ps.setString(1, person.getFirstName());
            ps.setString(2, person.getLastName());
            ps.setTimestamp(3, Timestamp.valueOf(person.getDob().withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime()));

            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();

            while (rs.next()) {
                long id = rs.getLong(1);

                person.setId(id);
                System.out.println(person);
            }

            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: " + person);
        }

        return person;
    }

    public Person findPersonById(Long id) {
        Person person = null;

        try {
            PreparedStatement ps = connection.prepareStatement("SELECT ID, FIRST_NAME, LAST_NAME, DOB FROM PEOPLE WHERE ID=?");

            ps.setLong(1, id);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                long personId = rs.getLong("ID");
                String firstName = rs.getString("FIRST_NAME");
                String lastName = rs.getString("LAST_NAME");
                ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));

                person = new Person(firstName, lastName, dob);
                person.setId(personId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return person;
    }
}
