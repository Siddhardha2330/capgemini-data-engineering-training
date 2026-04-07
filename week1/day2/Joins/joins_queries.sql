CREATE TABLE employees (
emp_id INT PRIMARY KEY,
emp_name VARCHAR(50),
manager_id INT,
dept_id INT
);
INSERT INTO employees (emp_id, emp_name, manager_id, dept_id) VALUES
(1, 'Karthik', NULL, 1),
(2, 'Ajay', 1, 1),
(3, 'Vijay', 1, 2),
(4, 'Vinay', 2, 2),
(5, 'Meena', 3, 3),
(6, 'Veer', NULL, 4),
(7, 'Keerthi', 4, 5),
(8, 'Priya', 4, 5);


CREATE TABLE departments (
dept_id INT PRIMARY KEY,
dept_name VARCHAR(50)
);

INSERT INTO departments (dept_id, dept_name) VALUES
(1, 'HR'),
(2, 'IT'),
(3, 'Finance'),
(4, 'Marketing'),
(5, 'Sales');

CREATE TABLE projects (
project_id INT PRIMARY KEY,
project_name VARCHAR(50),
emp_id INT
);

INSERT INTO projects (project_id, project_name, emp_id) VALUES
(1, 'Project A', 1),
(2, 'Project B', 2),
(3, 'Project C', 3),
(4, 'Project D', 4),
(5, 'Project E', 5);

--1. Retrieve the names of employees and their corresponding managers from the "employees" table, ensuring that even employees without managers are included.
select e1.emp_id,e1.emp_name,e2.emp_name as manager_name from employees e1 left join employees e2 on e1.emp_id=e2.emp_id

--2. Display all employees and their corresponding departments from the "employees" and "departments" tables, showing employees even if they don't belong to any department.
select e.emp_name,d.dept_name from employees e left join departments d on e.dept_id=d.dept_id

--3. List the names of employees who report to a manager, along with their manager's name, from the "employees" table.
select e.emp_name,m.emp_name as manager_name from employees e join employees m on e.manager_id=m.emp_id

-- 5. Display a list of employees who do not belong to any department, even if the department data is missing.
select e.emp_id,e.emp_name from employees e left join departments d on e.dept_id=d.dept_id where d.dept_id is NULL

--6. Fetch the names of employees and the projects they are assigned to. For employees who are not assigned any projects, show NULL for the project.
select e.emp_name,p.project_name from employees e left join project p on e.emp_id=p.emp_id

--7. List all employees who have completed at least one project, showing their names and the project names.
select e.emp_name,p.project_name from employees e join project p on e.emp_id=p.emp_id

--8. Show the names of employees and their projects, ensuring that no project is omitted even if an employee is not assigned to it.
select e.emp_name,p.project_name from projects p left join employees e on p.emp_id=e.emp_id

--11. Find the names of all departments and employees, ensuring that departments with no employees are included.
select d.dept_name,e.emp_name from departments d left join employees e on d.dept_id= e.dept_id

--13. Show the names of employees and their department names, including employees not assigned to any department and departments without employees.
select e.emp_name,d.dept_name from employees e inner join departments d on e.dept_id=d.dept_id

--14. Find employees who have not completed any project, along with the project details where applicable.
select e.emp_name,p.project_name from employees e left join projects p on e.emp_id=p.emp_id where p.project_id is NULL

--15. Retrieve the names of employees and the names of their projects, including employees who are not working on any project.
select e.emp_name,p.project_name from employees e left join projects p on e.emp_id=p.emp_id

--16. List all projects and the employees assigned to them, even for projects that have no employees.
select p.project_name,e.emp_name from projects p left join employees e on p.emp_id=e.emp_id

--17. Show the names of all employees who have both a manager and at least one project, listing the manager's name as well.
select e.emp_name,m.emp_name as manager_name from employees e join employees m on e.manager_id=m.emp_id join projects p on e.emp_id=p.emp_id

--18. List the names of employees and the corresponding department names, but exclude those employees who don't belong to a department.
select e.emp_name,d.dept_name from employees e join departments d on e.dept_id=d.dept_id
--19. Display employees who belong to multiple departments, showing the employee's name and the department names.
select e.emp_name,d.dept_name from employees e join departments d on e.dept_id=d.dept_id

--20. List the names of all departments and employees, ensuring that even if a department has no employees, it is included in the result.
select d.dept_name,e.emp_name from departments d left join employees e on d.dept_id=e.dept_id

--21. Retrieve employees who have worked on at least one project and do not belong to a department, listing their name and project details.
select e.emp_name,p.project_name from employees e left join departments d on e.dept_id=d.dept_id left join projects p on e.emp_id=p.emp_id where d.dept_id is NULL

--22. Find the total number of employees who belong to a department, ensuring the departments with no employees are still included.
select d.dept_name,count(e.emp_id) as total_employees from departments d left join employees e on d.dept_id=e.dept_id group by d.dept_name

--23. Show the employees and their managers, displaying only those employees who report to a manager, excluding employees without managers.
select e.emp_name,m.emp_name as manager_name from employees e join employees m on e.manager_id=m.emp_id

--24. Display all employee names along with their corresponding managers' names, but include employees who do not have managers.
select e.emp_name,m.emp_name as manager_name from employees e left join employees m on e.manager_id=m.emp_id

--25. Find the names of departments and the number of employees in each department, including departments that have no employees.
select d.dept_name,count(e.emp_id) as total_employees from departments d left join employees e on d.dept_id=e.dept_id group by d.dept_name

--26. List all employees and the departments they belong to, ensuring that departments with no employees are also listed.
select e.emp_name,d.dept_name from employees e left join departments d on e.dept_id=d.dept_id

--27. Show a list of employees who do not have any corresponding salary records, along with their names.
select e.emp_name from employees e left join salaries s on e.emp_id=s.emp_id where s.emp_id is NULL

--28. Retrieve the names of employees and their project assignments, including employees who are not assigned to any projects.
select e.emp_name,p.project_name from employees e left join projects p on e.emp_id=p.emp_id

--29. List the names of all employees and their respective department and project assignments, including employees who are not assigned to a project or department.
select e.emp_name,d.dept_name,p.project_name from employees e left join departments d on e.dept_id=d.dept_id left join projects p on e.emp_id=p.emp_id

--30. Display the names of employees who belong to at least one department, with the department name listed, but include employees without a department as well
select e.emp_name,d.dept_name from employees e left join departments d on e.dept_id=d.dept_id



