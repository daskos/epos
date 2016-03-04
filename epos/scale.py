# auto-scaling logic / PID controller


def pid_controller(y, yc, h=1, Ti=1, Td=1, Kp=1, u0=0, e0=0):
    """Calculate System Input using a PID Controller

    Arguments:
    y  .. Measured Output of the System
    yc .. Desired Output of the System
    h  .. Sampling Time
    Kp .. Controller Gain Constant
    Ti .. Controller Integration Constant
    Td .. Controller Derivation Constant
    u0 .. Initial state of the integrator
    e0 .. Initial error

    Make sure this function gets called every h seconds!
    """

    # Step variable
    k = 0

    # Initialization
    ui_prev = u0
    e_prev = e0

    while True:
        # Error between the desired and actual output
        e = yc - y

        # Integration Input
        ui = ui_prev + 1 / Ti * h * e
        # Derivation Input
        ud = 1 / Td * (e - e_prev) / h

        # Adjust previous values
        e_prev = e
        ui_prev = ui

        # Calculate input for the system
        u = Kp * (e + ui + ud)
        k += 1

        yield u
