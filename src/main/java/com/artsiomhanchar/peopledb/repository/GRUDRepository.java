package com.artsiomhanchar.peopledb.repository;

import com.artsiomhanchar.peopledb.exeption.UnableToSaveException;
import com.artsiomhanchar.peopledb.model.Entity;
import com.artsiomhanchar.peopledb.model.Person;

import java.sql.*;
import java.util.Optional;

abstract public class GRUDRepository<T extends Entity> {

    protected Connection connection;

    public GRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) throws UnableToSaveException {
//        String sql = String.format(
//                "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES('%s', '%s', '%s')",
//                person.getFirstName(), person.getLastName(), person.getDob()
//        );

        try {
            PreparedStatement ps = connection.prepareStatement(getSaveSQL(), Statement.RETURN_GENERATED_KEYS);

            mapForSave(entity, ps);

            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();

            while (rs.next()) {
                long id = rs.getLong(1);

                entity.setId(id);
                System.out.println(entity);
            }

            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: " + entity);
        }

        return entity;
    }

    public Optional<T> findById(Long id) {
        T entity = null;

        try {
            PreparedStatement ps = connection.prepareStatement(getFindByIdSQL());

            ps.setLong(1, id);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return Optional.ofNullable(entity);
    }

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    /**
     *
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", thaw will bind to the
     * entity's ID.
     */
    protected abstract String getFindByIdSQL();

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract String getSaveSQL();
}
