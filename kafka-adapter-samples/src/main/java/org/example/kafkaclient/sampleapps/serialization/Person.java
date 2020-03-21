package org.example.kafkaclient.sampleapps.serialization;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString
@RequiredArgsConstructor
public class Person {
    @Getter
    private final String firstName;

    @Getter
    private final String lastName;

    @Getter
    private final String userName;
}
