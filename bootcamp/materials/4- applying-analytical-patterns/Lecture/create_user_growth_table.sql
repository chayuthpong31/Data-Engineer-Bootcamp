CREATE TABLE users_growth_accounting(
	user_id TEXT,
	first_active_date TEXT,
	last_active_date DATE,
	daily_active_state TEXT,
	weekly_active_state TEXT,
	date_active DATE[],
	date DATE,
	PRIMARY KEY (user_id, date)
)