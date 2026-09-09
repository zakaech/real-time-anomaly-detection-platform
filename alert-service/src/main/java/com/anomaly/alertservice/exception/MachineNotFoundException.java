package com.anomaly.alertservice.exception;

public class MachineNotFoundException extends RuntimeException {

    private final String machineCode;

    public MachineNotFoundException(String machineCode) {
        super("Machine " + machineCode + " does not exist");
        this.machineCode = machineCode;
    }

    public String getMachineCode() {
        return machineCode;
    }
}
