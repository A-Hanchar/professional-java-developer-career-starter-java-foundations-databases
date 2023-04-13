package com.artsiomhanchar.peopledb.repoasitory;

import com.artsiomhanchar.peopledb.model.Person;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTest {
    @Test
    public void canSave() {
        PeopleRepository repo = new PeopleRepository();

        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 00, 0, ZoneId.of("-6")));

        Person savedPerson = repo.save(john);

        assertThat(savedPerson.getId()).isGreaterThan(0);
    }
}
