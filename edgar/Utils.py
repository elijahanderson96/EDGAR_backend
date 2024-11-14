from datetime import datetime


def generate_quarterly_periods(start_year=2010):
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_quarter = (current_month - 1) // 3 + 1

    periods = []

    # Generate periods from start_year to current year
    for year in range(start_year, current_year + 1):
        for quarter in range(1, 5):
            # Stop if we reach the current quarter of the current year
            if year == current_year and quarter > current_quarter:
                break
            periods.append(f"CY{year}Q{quarter}")

    return periods


quarterly_periods = generate_quarterly_periods()
