/*QUESTION 1*/
SELECT courses.code, courses.semester, courses.section, nums.num
FROM courses
INNER JOIN  
(SELECT courseid, COUNT(quizid) AS num 
                FROM quiz_course_close_assoc 
                GROUP BY courseid) nums 
ON courses.courseid = nums.courseid
ORDER BY nums.num DESC
LIMIT 1;

/*QUESTION 2*/
SELECT c.code, c.semester, c.section, qca.courseid, COUNT(lambdas.lambda_question_id) + COUNT(mcs.mc_question_id) + COUNT(exp.expr_question_id) + COUNT(func.func_question_id) AS questions
FROM quiz_course_close_assoc qca
LEFT JOIN lambda_assoc lambdas
ON qca.quizid = lambdas.quizid
LEFT JOIN multiple_choice_assoc mcs
ON qca.quizid = mcs.quizid
LEFT JOIN expression_assoc exp
ON qca.quizid = exp.quizid
LEFT JOIN function_assoc func
ON qca.quizid = func.quizid
LEFT JOIN courses c
ON c.courseid = qca.courseid
GROUP BY qca.courseid
ORDER BY questions DESC
LIMIT 1; 


/*QUESTION 3*/
SELECT uc.users_with_correct_answers, ua.users_with_attempts
FROM 
(SELECT COUNT(DISTINCT userid) AS users_with_correct_answers FROM code_answers WHERE question_type = 1 AND correct = 1) uc, 
(SELECT COUNT(DISTINCT userid) AS users_with_attempts FROM code_answers WHERE question_type = 1) ua;




/*CODE FOR QUESTION 3 THAT IS INTERESTING BUT NOT RELEVANT
SELECT COUNT(question_id) correct_answers 
FROM code_answers
WHERE question_type = 1
AND correct = 1;


SELECT SUM(a.answers) attempts
FROM
(SELECT COUNT(DISTINCT question_id) answers
FROM code_answers
WHERE question_type = 1
GROUP BY userid) a;*/


/*QUESTION 4*/
SELECT COUNT(qca.courseid)  AS lambda_courses
FROM quiz_course_close_assoc qca
JOIN lambda_assoc lambdas
ON qca.quizid = lambdas.quizid
LEFT JOIN courses c
ON c.courseid = qca.courseid;

/*QUESTION 5*/
SELECT userid, COUNT(correct) total
FROM code_answers
WHERE correct = 1
GROUP BY userid
ORDER BY total DESC
LIMIT 1;

/*QUESTION 6*/
SELECT spec_type, COUNT(question_id) total
FROM variable_specifications
GROUP BY spec_type
ORDER BY total DESC
LIMIT 3;


/*QUESTION 7*/
SELECT
(SELECT COUNT(DISTINCT quizid) AS coding_quizzes
FROM code_answers) /
(SELECT COUNT(*) AS  number_of_quizzes FROM quizzes)
AS percent_of_quizzes_with_coding_questions;


/*QUESTION 8*/
SELECT AVG(qs.questions) average_mc_questions_per_quiz
FROM 
(SELECT quizzes.quizid, COUNT(mc_question_id) questions
FROM quizzes
LEFT JOIN multiple_choice_assoc mc
ON quizzes.quizid = mc.quizid
GROUP BY quizzes.quizid) qs;


/*QUESTION 9*/
SELECT AVG(nums.num) AS average_coding_questions
FROM
(SELECT q.quizid, c.question_type, COUNT(DISTINCT c.question_id) AS num
FROM quizzes q
LEFT JOIN code_answers c
ON q.quizid = c.quizid
GROUP BY q.quizid, c.question_type) nums;

/*QUESTION 10*/
SELECT userid, MAX(c) attempts
FROM
(SELECT userid, quizid, COUNT(quizid) AS c
FROM code_answers
GROUP BY userid, quizid) attempts
GROUP BY userid
ORDER BY attempts DESC
LIMIT 1;




