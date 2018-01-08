package entities;

import java.util.UUID;

public class Pet {
    public UUID petId;
    public String name;
    public String type;

    @Override
    public String toString() {
        return "Pet{" +
                "petId=" + petId +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    public Pet() {
    }
}
