package com.artsiomhanchar.peopledb.repository;

import com.artsiomhanchar.peopledb.exeption.UnableToSaveException;
import com.artsiomhanchar.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();

        try {
            PreparedStatement ps = connection.prepareStatement(getFindAllSQL());
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return entities;
    }

    public long count() {
        long count = 0;

        try {
            PreparedStatement ps = connection.prepareStatement(getCountSQL());
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getDeleteSQL());

            ps.setLong(1, entity.getId());

            int affectedRecordCount = ps.executeUpdate();

            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(T...entities) { // T[] entities
//        for(T entity : entities) {
//            delete(entity);
//        }

        try {
            Statement stmt = connection.createStatement();

            String ids = Arrays.stream(entities)
                    .map(T::getId)
                    .map(String::valueOf) // 10L => "10"
                    .collect(
                            Collectors.joining(",")
                    );

            int affectedRecordCount = stmt.executeUpdate(getDeleteInSQL().replace(":ids", ids));

            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getUpdateSQL());

            mapForUpdate(entity, ps);

            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract String getUpdateSQL();

    protected abstract String getDeleteSQL();

    /**
     *
     * @return SHOULD return a SQL string like:
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the '(:ids)' named parameter & call it 'ids'
     */
    protected abstract String getDeleteInSQL();

    protected abstract String getCountSQL();

    protected abstract String getFindAllSQL();

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    /**
     *
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", thaw will bind to the
     * entity's ID.
     */
    protected abstract String getFindByIdSQL();

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;

    abstract String getSaveSQL();
}
