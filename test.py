# Calculate growth rate from 4.8K to 2.5M ARR in 11 months

# Initial and final values
initial_value = 4.8 * 1000  # 4.8K
final_value = 2.5 * 1000000  # 2.5M ARR
time_period = 11             # 11 months

# Calculate the monthly growth rate
# Using the formula: final_value = initial_value * (1 + growth_rate)^time_period
# Solving for growth_rate: growth_rate = (final_value/initial_value)^(1/time_period) - 1

monthly_growth_rate = (final_value / initial_value) ** (1 / time_period) - 1
monthly_growth_percentage = monthly_growth_rate * 100

# Calculate the overall growth percentage
overall_growth_percentage = ((final_value - initial_value) / initial_value) * 100



print(f"Initial value: {initial_value:,.0f}")
print(f"Final value: {final_value:,.0f}")
print(f"Time period: {time_period} months")
print(f"Monthly growth rate: {monthly_growth_rate:.4f} ({monthly_growth_percentage:.2f}%)")
print(f"Overall growth: {overall_growth_percentage:.2f}%")
