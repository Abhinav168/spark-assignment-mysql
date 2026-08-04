-- Library Analytics schema + seed data for Spark 101 take-home assignment.
-- Fixed "as of" date used by overdue-style analytics: 2025-07-15
-- (loans/fines are dated within their own synthetic timeline, not wall-clock "today").

CREATE TABLE IF NOT EXISTS authors (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  birth_year INT,
  country VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS books (
  id INT PRIMARY KEY AUTO_INCREMENT,
  title VARCHAR(150) NOT NULL,
  author_id INT NOT NULL,
  genre VARCHAR(50) NOT NULL,
  price DECIMAL(6,2) NOT NULL,
  published_year INT,
  FOREIGN KEY (author_id) REFERENCES authors(id)
);

CREATE TABLE IF NOT EXISTS members (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(150) NOT NULL,
  country VARCHAR(50),
  joined_date DATE
);

CREATE TABLE IF NOT EXISTS loans (
  id INT PRIMARY KEY AUTO_INCREMENT,
  book_id INT NOT NULL,
  member_id INT NOT NULL,
  loan_date DATE NOT NULL,
  due_date DATE NOT NULL,
  return_date DATE,
  status ENUM('ACTIVE', 'RETURNED', 'OVERDUE') NOT NULL,
  FOREIGN KEY (book_id) REFERENCES books(id),
  FOREIGN KEY (member_id) REFERENCES members(id)
);

CREATE TABLE IF NOT EXISTS fines (
  id INT PRIMARY KEY AUTO_INCREMENT,
  loan_id INT NOT NULL,
  amount DECIMAL(6,2) NOT NULL,
  status ENUM('PAID', 'UNPAID') NOT NULL,
  created_date DATE NOT NULL,
  paid_date DATE,
  FOREIGN KEY (loan_id) REFERENCES loans(id)
);

INSERT INTO authors (name, birth_year, country) VALUES
  ('George Orwell', 1903, 'UK'),
  ('Isaac Asimov', 1920, 'USA'),
  ('Agatha Christie', 1890, 'UK'),
  ('Chimamanda Ngozi Adichie', 1977, 'Nigeria'),
  ('Haruki Murakami', 1949, 'Japan'),
  ('Yuval Noah Harari', 1976, 'Israel'),
  ('J.K. Rowling', 1965, 'UK'),
  ('Toni Morrison', 1931, 'USA'),
  ('Gabriel Garcia Marquez', 1927, 'Colombia'),
  ('Margaret Atwood', 1939, 'Canada');

INSERT INTO books (title, author_id, genre, price, published_year) VALUES
  ('1984', 1, 'Fiction', 12.99, 1949),
  ('Animal Farm', 1, 'Fiction', 9.99, 1945),
  ('Foundation', 2, 'Sci-Fi', 14.99, 1951),
  ('I, Robot', 2, 'Sci-Fi', 13.50, 1950),
  ('Murder on the Orient Express', 3, 'Mystery', 10.99, 1934),
  ('And Then There Were None', 3, 'Mystery', 11.50, 1939),
  ('Americanah', 4, 'Fiction', 15.99, 2013),
  ('Half of a Yellow Sun', 4, 'Fiction', 14.50, 2006),
  ('Norwegian Wood', 5, 'Fiction', 13.99, 1987),
  ('Kafka on the Shore', 5, 'Fantasy', 16.99, 2002),
  ('Sapiens', 6, 'Non-Fiction', 18.99, 2011),
  ('Homo Deus', 6, 'Non-Fiction', 19.99, 2016),
  ('Harry Potter and the Sorcerer''s Stone', 7, 'Fantasy', 17.99, 1997),
  ('Harry Potter and the Chamber of Secrets', 7, 'Fantasy', 17.99, 1998),
  ('Beloved', 8, 'Fiction', 12.50, 1987),
  ('Song of Solomon', 8, 'Fiction', 11.99, 1977),
  ('One Hundred Years of Solitude', 9, 'Fiction', 16.50, 1967),
  ('Love in the Time of Cholera', 9, 'Fiction', 15.50, 1985),
  ('The Handmaid''s Tale', 10, 'Fiction', 14.99, 1985),
  ('Oryx and Crake', 10, 'Sci-Fi', 15.99, 2003);

INSERT INTO members (name, email, country, joined_date) VALUES
  ('Alice Smith', 'alice.smith@example.com', 'USA', '2023-01-15'),
  ('Bob Jones', 'bob.jones@example.com', 'UK', '2023-02-20'),
  ('Carol White', 'carol.white@example.com', 'Canada', '2023-03-10'),
  ('David Lee', 'david.lee@example.com', 'Australia', '2023-04-05'),
  ('Eva Green', 'eva.green@example.com', 'India', '2023-05-12'),
  ('Frank Brown', 'frank.brown@example.com', 'USA', '2023-06-18'),
  ('Grace Kim', 'grace.kim@example.com', 'UK', '2023-07-22'),
  ('Henry Wilson', 'henry.wilson@example.com', 'Canada', '2023-08-01'),
  ('Ivy Chen', 'ivy.chen@example.com', 'India', '2023-08-15'),
  ('Jack Davis', 'jack.davis@example.com', 'USA', '2023-09-10'),
  ('Karen Clark', 'karen.clark@example.com', 'Australia', '2023-09-25'),
  ('Liam Walker', 'liam.walker@example.com', 'UK', '2023-10-05'),
  ('Mia Hall', 'mia.hall@example.com', 'India', '2023-10-20'),
  ('Noah Young', 'noah.young@example.com', 'USA', '2023-11-01'),
  ('Olivia King', 'olivia.king@example.com', 'Canada', '2023-11-15');

INSERT INTO loans (book_id, member_id, loan_date, due_date, return_date, status) VALUES
  (1, 1, '2025-01-05', '2025-01-19', '2025-01-15', 'RETURNED'),
  (3, 2, '2025-01-06', '2025-01-20', '2025-01-25', 'RETURNED'),
  (5, 3, '2025-01-08', '2025-01-22', '2025-01-20', 'RETURNED'),
  (7, 4, '2025-01-10', '2025-01-24', '2025-01-30', 'RETURNED'),
  (11, 5, '2025-01-12', '2025-01-26', '2025-01-24', 'RETURNED'),
  (13, 6, '2025-01-15', '2025-01-29', '2025-01-29', 'RETURNED'),
  (2, 7, '2025-02-02', '2025-02-16', '2025-02-14', 'RETURNED'),
  (4, 8, '2025-02-03', '2025-02-17', '2025-02-25', 'RETURNED'),
  (6, 9, '2025-02-05', '2025-02-19', '2025-02-18', 'RETURNED'),
  (8, 10, '2025-02-07', '2025-02-21', '2025-02-20', 'RETURNED'),
  (12, 11, '2025-02-10', '2025-02-24', '2025-02-22', 'RETURNED'),
  (14, 12, '2025-02-12', '2025-02-26', '2025-03-02', 'RETURNED'),
  (9, 13, '2025-03-01', '2025-03-15', '2025-03-14', 'RETURNED'),
  (10, 14, '2025-03-03', '2025-03-17', '2025-03-16', 'RETURNED'),
  (15, 15, '2025-03-05', '2025-03-19', '2025-03-25', 'RETURNED'),
  (16, 1, '2025-03-08', '2025-03-22', '2025-03-21', 'RETURNED'),
  (17, 2, '2025-03-10', '2025-03-24', '2025-03-23', 'RETURNED'),
  (18, 3, '2025-03-12', '2025-03-26', '2025-04-01', 'RETURNED'),
  (19, 4, '2025-04-01', '2025-04-15', '2025-04-13', 'RETURNED'),
  (20, 5, '2025-04-03', '2025-04-17', '2025-04-16', 'RETURNED'),
  (1, 6, '2025-04-05', '2025-04-19', '2025-04-27', 'RETURNED'),
  (3, 7, '2025-04-08', '2025-04-22', '2025-04-20', 'RETURNED'),
  (5, 8, '2025-04-10', '2025-04-24', '2025-04-23', 'RETURNED'),
  (7, 9, '2025-04-12', '2025-04-26', NULL, 'OVERDUE'),
  (11, 10, '2025-05-01', '2025-05-15', '2025-05-14', 'RETURNED'),
  (13, 11, '2025-05-03', '2025-05-17', '2025-05-25', 'RETURNED'),
  (2, 12, '2025-05-05', '2025-05-19', '2025-05-18', 'RETURNED'),
  (4, 13, '2025-05-08', '2025-05-22', NULL, 'OVERDUE'),
  (6, 14, '2025-05-10', '2025-05-24', '2025-05-23', 'RETURNED'),
  (8, 15, '2025-05-12', '2025-05-26', '2025-05-30', 'RETURNED'),
  (12, 1, '2025-06-01', '2025-06-15', '2025-06-14', 'RETURNED'),
  (14, 2, '2025-06-03', '2025-06-17', NULL, 'OVERDUE'),
  (9, 3, '2025-06-05', '2025-06-19', '2025-06-18', 'RETURNED'),
  (10, 4, '2025-06-08', '2025-06-22', NULL, 'OVERDUE'),
  (15, 5, '2025-06-10', '2025-06-24', '2025-06-23', 'RETURNED'),
  (16, 6, '2025-06-12', '2025-06-26', '2025-07-02', 'RETURNED'),
  (17, 7, '2025-07-01', '2025-07-20', NULL, 'ACTIVE'),
  (18, 8, '2025-07-03', '2025-07-22', NULL, 'ACTIVE'),
  (19, 9, '2025-07-05', '2025-07-24', NULL, 'ACTIVE'),
  (20, 10, '2025-07-08', '2025-07-27', NULL, 'ACTIVE');

INSERT INTO fines (loan_id, amount, status, created_date, paid_date) VALUES
  (2, 2.50, 'PAID', '2025-01-25', '2025-01-26'),
  (4, 3.00, 'PAID', '2025-01-30', '2025-02-02'),
  (8, 4.00, 'UNPAID', '2025-02-25', NULL),
  (12, 2.00, 'PAID', '2025-03-02', '2025-03-05'),
  (15, 3.00, 'PAID', '2025-03-25', '2025-03-28'),
  (18, 3.00, 'UNPAID', '2025-04-01', NULL),
  (21, 4.00, 'PAID', '2025-04-27', '2025-04-30'),
  (26, 4.00, 'UNPAID', '2025-05-25', NULL),
  (30, 2.00, 'PAID', '2025-05-30', '2025-06-02'),
  (36, 3.00, 'UNPAID', '2025-07-02', NULL),
  (24, 40.00, 'UNPAID', '2025-04-27', NULL),
  (28, 27.00, 'UNPAID', '2025-05-23', NULL),
  (32, 14.00, 'UNPAID', '2025-06-18', NULL),
  (34, 11.50, 'UNPAID', '2025-06-23', NULL);
