package com.artsiomhanchar.peopledb.repository;

import com.artsiomhanchar.peopledb.annotation.Id;
import com.artsiomhanchar.peopledb.annotation.MultiSQL;
import com.artsiomhanchar.peopledb.annotation.SQL;
import com.artsiomhanchar.peopledb.exeption.UnableToSaveException;
import com.artsiomhanchar.peopledb.model.CrudOperation;
import com.artsiomhanchar.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract public class GRUDRepository<T> {

    protected Connection connection;

    public GRUDRepository(Connection connection) {
        this.connection = connection;
    }

    private String getSQLByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        Stream<SQL> multiSQLStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(MultiSQL.class))
                .map(method -> method.getAnnotation(MultiSQL.class))
                .flatMap(multiSQL -> Arrays.stream(multiSQL.value()));

        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(SQL.class))
                .map(method -> method.getAnnotation(SQL.class));

        return Stream.concat(multiSQLStream, sqlStream)
                .filter(annotation -> annotation.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst()
                .orElseGet(sqlGetter);
    }

    public T save(T entity) throws UnableToSaveException {
//        String sql = String.format(
//                "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES('%s', '%s', '%s')",
//                person.getFirstName(), person.getLastName(), person.getDob()
//        );

        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.SAVE, this::getSaveSQL), Statement.RETURN_GENERATED_KEYS);

            mapForSave(entity, ps);

            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();

            while (rs.next()) {
                long id = rs.getLong(1);

                setIdByAnnotation(id, entity);
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
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSQL));

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
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSQL));
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
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.COUNT, this::getCountSQL));
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
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteSQL));

            ps.setLong(1, getIdByAnnotation(entity));

            int affectedRecordCount = ps.executeUpdate();

            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Long getIdByAnnotation(T entity) {
        return Arrays.stream(entity
                .getClass()
                .getDeclaredFields()
        )
                .filter(field -> field.isAnnotationPresent(Id.class))
                .map(field -> {
                    field.setAccessible(true);

                    Long id= null;

                    try {
                        id = (long) field.get(entity);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }

                    return id;
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No ID annotated field found"));
    }

    private void setIdByAnnotation(Long id, T entity) {
        Arrays.stream(entity
                        .getClass()
                        .getDeclaredFields()
                )
                .filter(field -> field.isAnnotationPresent(Id.class))
                .forEach(field -> {
                    field.setAccessible(true);

                    try {
                        field.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set ID field value.");
                    }
                });
    }

    public void delete(T...entities) { // T[] entities
//        for(T entity : entities) {
//            delete(entity);
//        }

        try {
            Statement stmt = connection.createStatement();

            String ids = Arrays.stream(entities)
//                    .map(entity -> findIdByAnnotation(entity))
                    .map(this::getIdByAnnotation)
                    .map(String::valueOf) // 10L => "10"
                    .collect(
                            Collectors.joining(",")
                    );

            int affectedRecordCount = stmt.executeUpdate(getSQLByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteInSQL).replace(":ids", ids));

            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.UPDATE, this::getUpdateSQL));

            mapForUpdate(entity, ps);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getUpdateSQL() {
        throw new RuntimeException("SQL not defined");
    };

    protected String getDeleteSQL(){
        throw new RuntimeException("SQL not defined");
    };

    /**
     *
     * @return SHOULD return a SQL string like:
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the '(:ids)' named parameter & call it 'ids'
     */
    protected String getDeleteInSQL(){
        throw new RuntimeException("SQL not defined");
    };

    protected String getCountSQL(){
        throw new RuntimeException("SQL not defined");
    };

    protected String getFindAllSQL(){
        throw new RuntimeException("SQL not defined");
    };

    /**
     *
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", thaw will bind to the
     * entity's ID.
     */
    protected String getFindByIdSQL() {
        throw new RuntimeException("SQL not defined");
    };

    String getSaveSQL() {
        throw new RuntimeException("SQL not defined");
    };

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;
}
