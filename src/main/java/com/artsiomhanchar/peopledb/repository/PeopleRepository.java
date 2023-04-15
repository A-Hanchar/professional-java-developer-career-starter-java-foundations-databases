package com.artsiomhanchar.peopledb.repository;

import com.artsiomhanchar.peopledb.annotation.SQL;
import com.artsiomhanchar.peopledb.model.Address;
import com.artsiomhanchar.peopledb.model.CrudOperation;
import com.artsiomhanchar.peopledb.model.Person;
import com.artsiomhanchar.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.Set;

public class PeopleRepository extends GrudRepository<Person> {
    private AddressRepository addressRepository = null;

    public static final String SAVE_PERSON_SQL = """
            INSERT INTO PEOPLE 
            (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BUSINESS_ADDRESS, PARENT_ID) 
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """;
//    public static final String FIND_BY_ID_SQL = """
//            SELECT
//            P.ID, P.FIRST_NAME, P.LAST_NAME, P.DOB, P.SALARY, P.HOME_ADDRESS,
//            HOME_A.ID AS HOME_A_ID, HOME_A.STREET_ADDRESS AS HOME_A_STREET_ADDRESS, HOME_A.ADDRESS2 AS HOME_A_ADDRESS2, HOME_A.CITY AS HOME_A_CITY, HOME_A.STATE AS HOME_A_STATE, HOME_A.POSTCODE AS HOME_A_POSTCODE, HOME_A.COUNTY AS HOME_A_COUNTY, HOME_A.REGION AS HOME_A_REGION, HOME_A.COUNTRY AS HOME_A_COUNTRY,
//            BUSINESS_A.ID AS BUSINESS_A_ID, BUSINESS_A.STREET_ADDRESS AS BUSINESS_A_STREET_ADDRESS, BUSINESS_A.ADDRESS2 AS BUSINESS_A_ADDRESS2, BUSINESS_A.CITY AS BUSINESS_A_CITY, BUSINESS_A.STATE AS BUSINESS_A_STATE, BUSINESS_A.POSTCODE AS BUSINESS_A_POSTCODE, BUSINESS_A.COUNTY AS BUSINESS_A_COUNTY, BUSINESS_A.REGION AS BUSINESS_A_REGION, BUSINESS_A.COUNTRY AS BUSINESS_A_COUNTRY
//            FROM PEOPLE AS P
//            LEFT OUTER JOIN ADDRESSES AS HOME_A ON P.HOME_ADDRESS = HOME_A.ID
//            LEFT OUTER JOIN ADDRESSES AS BUSINESS_A ON P.BUSINESS_ADDRESS = BUSINESS_A.ID
//            WHERE P.ID=?
//            """;
    public static final String FIND_BY_ID_SQL = """
            SELECT 
            PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME, PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.EMAIL AS PARENT_EMAIL,
            CHILD.ID AS CHILD_ID, CHILD.FIRST_NAME AS CHILD_FIRST_NAME, CHILD.LAST_NAME AS CHILD_LAST_NAME, CHILD.DOB AS CHILD_DOB, CHILD.SALARY AS CHILD_SALARY, CHILD.EMAIL AS CHILD_EMAIL,
            HOME_A.ID AS HOME_A_ID, HOME_A.STREET_ADDRESS AS HOME_A_STREET_ADDRESS, HOME_A.ADDRESS2 AS HOME_A_ADDRESS2, HOME_A.CITY AS HOME_A_CITY, HOME_A.STATE AS HOME_A_STATE, HOME_A.POSTCODE AS HOME_A_POSTCODE, HOME_A.COUNTY AS HOME_A_COUNTY, HOME_A.REGION AS HOME_A_REGION, HOME_A.COUNTRY AS HOME_A_COUNTRY,
            BUSINESS_A.ID AS BUSINESS_A_ID, BUSINESS_A.STREET_ADDRESS AS BUSINESS_A_STREET_ADDRESS, BUSINESS_A.ADDRESS2 AS BUSINESS_A_ADDRESS2, BUSINESS_A.CITY AS BUSINESS_A_CITY, BUSINESS_A.STATE AS BUSINESS_A_STATE, BUSINESS_A.POSTCODE AS BUSINESS_A_POSTCODE, BUSINESS_A.COUNTY AS BUSINESS_A_COUNTY, BUSINESS_A.REGION AS BUSINESS_A_REGION, BUSINESS_A.COUNTRY AS BUSINESS_A_COUNTRY
            FROM PEOPLE AS PARENT
            LEFT OUTER JOIN PEOPLE AS CHILD ON PARENT.ID = CHILD.PARENT_ID
            LEFT OUTER JOIN ADDRESSES AS HOME_A ON PARENT.HOME_ADDRESS = HOME_A.ID
            LEFT OUTER JOIN ADDRESSES AS BUSINESS_A ON PARENT.BUSINESS_ADDRESS = BUSINESS_A.ID
            WHERE PARENT.ID = ?
            """;
    public static final String FIND_ALL_SQL = """
            SELECT 
            PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME, PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.EMAIL AS PARENT_EMAIL 
            FROM PEOPLE AS PARENT
            FETCH FIRST 100 ROWS ONLY
            """;
    public static final String SELECT_COUNT_SQL = "SELECT COUNT(*) FROM PEOPLE";
    public static final String DELETE_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String DELETE_IN_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    public static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";

    public PeopleRepository(Connection connection) {
        super(connection);

        addressRepository = new AddressRepository(connection);
    }

    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person person, PreparedStatement ps) throws SQLException {
        Address savedAddress = null;

        ps.setString(1, person.getFirstName());
        ps.setString(2, person.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(person.getDob()));
        ps.setBigDecimal(4, person.getSalary());
        ps.setString(5, person.getEmail());

        associateAddressWithPerson(ps, person.getHomeAddress(), 6);
        associateAddressWithPerson(ps, person.getBusinessAddress(), 7);

        associateChildWithPerson(person, ps);
    }

    private void associateChildWithPerson(Person person, PreparedStatement ps) throws SQLException {
        Optional<Person> parent = person.getParent();

        if (parent.isPresent()) {
            ps.setLong(8, parent.get().getId());
        } else {
            ps.setObject(8, null);
        }
    }

    @Override
    protected void postSave(Person entity, long id) {
        entity
                .getChildren()
                .stream()
                .forEach(this::save);
    }

    private void associateAddressWithPerson(PreparedStatement ps, Optional<Address> address, int parameterIndex) throws SQLException {
        Address savedAddress;
        if (address.isPresent()) {
            savedAddress = addressRepository.save(address.get());
            ps.setLong(parameterIndex, savedAddress.id());
        } else {
            ps.setObject(parameterIndex, null);
        }
    }

    @Override
    @SQL(value = UPDATE_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person person, PreparedStatement ps) throws SQLException {
        ps.setString(1, person.getFirstName());
        ps.setString(2, person.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(person.getDob()));
        ps.setBigDecimal(4, person.getSalary());
        ps.setLong(5, person.getId());
    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = SELECT_COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_SQL, operationType = CrudOperation.DELETE_ONE)
    @SQL(value = DELETE_IN_SQL, operationType = CrudOperation.DELETE_MANY)
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        Person finalParent = null;

        do {
            Person currentParent = extractPerson(rs, "PARENT_").get();

            if (finalParent == null) {
                finalParent = currentParent;
            }

            if (!finalParent.equals(currentParent)) {
                rs.previous();
                break;
            }

            Optional<Person> child = extractPerson(rs, "CHILD_");

            Address homeAddress = extractAddress(rs, "HOME_A_");
            Address businessAddress = extractAddress(rs, "BUSINESS_A_");

            finalParent.setHomeAddress(homeAddress);
            finalParent.setBusinessAddress(businessAddress);

            child.ifPresent(finalParent::addChild);
        } while (rs.next());

        return finalParent;
    }

    private Optional<Person> extractPerson(ResultSet rs, String aliasPrefix) throws SQLException {
        Long personId = getValueByAlias(aliasPrefix + "ID", rs, Long.class);

        if (personId == null) {
            return Optional.empty();
        }

        String firstName = getValueByAlias(aliasPrefix + "FIRST_NAME", rs, String.class);
        String lastName = getValueByAlias(aliasPrefix + "LAST_NAME", rs, String.class);
        ZonedDateTime dob = ZonedDateTime.of(getValueByAlias(aliasPrefix + "DOB", rs, Timestamp.class).toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = getValueByAlias(aliasPrefix + "SALARY", rs, BigDecimal.class);

        Person person = new Person(personId, firstName, lastName, dob, salary);

        return Optional.of(person);
    }

    private Address extractAddress(ResultSet rs, String aliasPrefix) throws SQLException {
        Long addressId = getValueByAlias(aliasPrefix + "ID", rs, Long.class);

        if (addressId == null) {
            return null;
        }

        String streetAddress = getValueByAlias(aliasPrefix + "STREET_ADDRESS", rs, String.class);
        String address2 = getValueByAlias(aliasPrefix + "ADDRESS2", rs, String.class);
        String city = getValueByAlias(aliasPrefix + "CITY", rs, String.class);
        String state = getValueByAlias(aliasPrefix + "STATE", rs, String.class);
        String postcode = getValueByAlias(aliasPrefix + "POSTCODE", rs, String.class);
        String county = getValueByAlias(aliasPrefix + "COUNTY", rs, String.class);
        Region region = Region.valueOf(getValueByAlias(aliasPrefix + "REGION", rs, String.class).toUpperCase());
        String country = getValueByAlias(aliasPrefix + "COUNTRY", rs, String.class);

        Address address = new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);

        return address;
    }

    private <T> T getValueByAlias(String alias, ResultSet rs, Class<T> clazz) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();

        // Params:
        //columnIndex – the first column is 1, the second is 2, ...
        for (int colIdx = 1; colIdx <= columnCount; colIdx++) {
            if (alias.equals(rs.getMetaData().getColumnLabel(colIdx))) {
                return (T) rs.getObject(alias);
            }
        }

        return null;
//        throw new SQLException(String.format("Column not found for alias: '%s'%n", alias));
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
