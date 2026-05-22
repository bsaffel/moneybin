/*
@name seasonal_spending
@description Seasonal spending breakdown by category and year
@param year INTEGER optional default=null "Filter to specific year (null = all years)"
@param categories TEXT[] optional default=null "Filter to specific categories"
@param min_amount DECIMAL optional default=0 "Minimum absolute amount"
@example reports_seasonal_spending(year=2025)
@example reports_seasonal_spending(categories=["Groceries", "Dining"])
*/
MODEL (name reports.seasonal_spending, kind VIEW);
SELECT 1
;
