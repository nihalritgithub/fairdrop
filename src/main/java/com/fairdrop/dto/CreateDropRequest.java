package com.fairdrop.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * Request body for {@code POST /drops}. Bean Validation constraints reject a blank name or
 * a non-positive inventory before the request reaches the service layer.
 */
public class CreateDropRequest {

    @NotBlank(message = "Drop name is required")
    private String name;

    @Positive(message = "Inventory must be greater than zero")
    private int inventory;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getInventory() {
        return inventory;
    }

    public void setInventory(int inventory) {
        this.inventory = inventory;
    }
}
