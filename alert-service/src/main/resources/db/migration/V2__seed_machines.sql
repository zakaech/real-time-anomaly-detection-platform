-- Machine roster, transcribed from event-simulator/config/fleet.yaml.
--
-- Only the `nominal` half of that file is reproduced here: machine_id, line_id
-- and profile are what a plant genuinely knows about its machines. The
-- `simulation` block (noise sigmas, coupling coefficients, wear rates) is the
-- hidden physics and must never leave the simulator -- copying it into the
-- platform's own database would be a leak into the design itself.
--
-- ON CONFLICT DO NOTHING because the ingestion path auto-provisions unknown
-- machines (decision D-39): this seed must be able to run after a machine has
-- already appeared through an alert, and enrich it rather than collide with it.

INSERT INTO machine (code, line_code, machine_type) VALUES
    ('M-001', 'LINE-A', 'spindle'),
    ('M-002', 'LINE-A', 'spindle'),
    ('M-003', 'LINE-A', 'pump'),
    ('M-004', 'LINE-A', 'conveyor'),
    ('M-005', 'LINE-A', 'spindle'),
    ('M-006', 'LINE-B', 'pump'),
    ('M-007', 'LINE-B', 'spindle'),
    ('M-008', 'LINE-B', 'conveyor'),
    ('M-009', 'LINE-B', 'pump'),
    ('M-010', 'LINE-B', 'spindle'),
    ('M-011', 'LINE-C', 'conveyor'),
    ('M-012', 'LINE-C', 'spindle'),
    ('M-013', 'LINE-C', 'pump'),
    ('M-014', 'LINE-C', 'spindle'),
    ('M-015', 'LINE-C', 'conveyor')
ON CONFLICT (code) DO UPDATE
    SET machine_type = EXCLUDED.machine_type,
        line_code    = EXCLUDED.line_code;
