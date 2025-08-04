class Employee:
    def __init__(self, name, hourly_rate):
        self.name = name
        self.hourly_rate = hourly_rate

    def calculate_salary(self, hours_worked, bonus=0):
        if hours_worked <= 40:
            salary = hours_worked * self.hourly_rate
        else:
            regular_pay = 40 * self.hourly_rate
            overtime_pay = (hours_worked - 40) * (self.hourly_rate * 1.5)
            salary = regular_pay + overtime_pay
        salary += bonus
        return salary

    def calculate_net_salary(self, hours_worked, bonus=0, state_tax_rate=0.056, fed_tax_rate=0.079):
        gross_salary = self.calculate_salary(hours_worked, bonus)
        state_tax = gross_salary * state_tax_rate
        federal_tax = gross_salary * fed_tax_rate
        return gross_salary - state_tax - federal_tax

# Test cases

def test_employee():
    # Case 1: under 40 hours
    emp = Employee("Alice", 10)
    gross = emp.calculate_salary(40)
    net = emp.calculate_net_salary(40)
    assert gross == 400
    assert abs(net - 400 * (1 - 0.056 - 0.079)) < 1e-6
    # Case 2: overtime
    emp2 = Employee("Bob", 10)
    gross2 = emp2.calculate_salary(50)
    assert gross2 == 40*10 + 10*10*1.5
    net2 = emp2.calculate_net_salary(50)
    assert abs(net2 - gross2 * (1 - 0.056 - 0.079)) < 1e-6
    print("Test cases passed.")

if __name__ == '__main__':
    test_employee()
