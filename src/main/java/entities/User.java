package entities;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

public class User {
    public UUID userId;
    public String login;
    public String name;
    public String password;
    public Set<UUID> pets = new LinkedHashSet<>();

    public User(UUID userId, String login, String name, String password, Set<UUID> pets) {
        this.userId = userId;
        this.login = login;
        this.name = name;
        this.password = password;
        this.pets = pets;
    }

    public User() {
    }

    @Override
    public String toString() {
        return "User{" +
                "userId='" + userId + '\'' +
                ", login='" + login + '\'' +
                ", name='" + name + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
