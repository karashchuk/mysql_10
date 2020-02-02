-- Домашнее задание к уроку 8

-- 1. Проанализировать какие запросы могут выполняться наиболее часто в процессе работы приложения и добавить необходимые индексы.

-- наиболее часто запрашиваемая таблица будет users, поэтому по ней необходимы индексы для поиска по имени и фамилии, а также по e-mail
CREATE INDEX first_name_last_name_idx ON users(first_name, last_name);
CREATE UNIQUE INDEX users_email_uq ON users(email);

-- для постоянной работы с лайками и ссылками на другие таблицы имеет смысл сделать составной индекс для ссылки на таблицы и id записи в талице;
CREATE INDEX target_type_id_target_id_idx ON likes(target_type_id, target_id);
-- аналогично для работы с медиафайлами:
CREATE INDEX media_user_id_media_type_id_idx ON media(user_id, media_type_id);
-- когда будет много порльзователей и необходима постоянная выборка по данным пользователям, например по его городу пребываня, то будет иметь смысл добавить индекс:

CREATE INDEX profiles_hometown_idx ON profiles(hometown);

-- 2. Задание на оконные функции
-- Построить запрос, который будет выводить следующие столбцы:
-- имя группы
-- среднее количество пользователей в группах
-- самый молодой пользователь в группе
-- самый пожилой пользователь в группе
-- общее количество пользователей в группе
-- всего пользователей в системе
--отношение в процентах (общее количество пользователей в группе / всего пользователей в системе) * 100

SELECT DISTINCT 
	c.name,
	(select avg(amount) from
		(SELECT DISTINCT 
			count(user_id) OVER(PARTITION BY community_id ) AS amount
		FROM communities_users) as ww
	) as average ,
	first_value(concat (u.first_name,' ', u.last_name )) over (partition by c.id order by p.birthday desc) as youngest,
	max(p.birthday ) OVER w AS y_birthday,
	first_value(concat (u.first_name,' ', u.last_name )) over (partition by c.id order by p.birthday) as oldest,
	min(p.birthday ) OVER w AS o_birthday,
	count(cu.user_id) OVER w AS total_by_group,
	count(c.id) OVER() AS total,
	count(cu.user_id) OVER w / count(c.id) OVER() * 100 AS "%%"
FROM (communities_users cu 
	JOIN communities c 
		ON cu.community_id = c.id )
	join users u 
		on u.id = cu.user_id
	join profiles p 
		on p.user_id = cu.user_id 
	WINDOW w AS (PARTITION BY c.id);


-- 3. (по желанию) Задание на денормализацию
-- Разобраться как построен и работает следующий запрос:
-- Найти 10 пользователей, которые проявляют наименьшую активность в использовании социальной сети.

SELECT 
	users.id,
	COUNT(DISTINCT messages.id) +
	COUNT(DISTINCT likes.id) +
	COUNT(DISTINCT media.id) AS activity
FROM users
	LEFT JOIN messages
		ON users.id = messages.from_user_id
	LEFT JOIN likes
		ON users.id = likes.user_id
	LEFT JOIN media
		ON users.id = media.user_id
GROUP BY users.id
ORDER BY activity
LIMIT 10;

-- Правильно-ли он построен?

-- если считать активность только по трем таблицам и и только по вставляемым строкам  - то запрос построен правильно, так как считает относительно таблицы users по left join. но в общем виде за активность можно считать не только эти таблицы и не только по операции insert, а также по update и delete.

-- Какие изменения, включая денормализацию, можно внести в структуру БД
-- чтобы существенно повысить скорость работы этого запроса?

-- для оптимизации именно этого запроса напрашивается самое просто решение, это добавить поле activity в таблицу users
alter table users add activity int unsigned default 0;
-- далее пишем триггер на каждую таблицу по которой меряем активность, например для likes для операции insert:
drop trigger if exists users_activity;
DELIMITER -
create trigger users_activity after insert on likes
for each row BEGIN
	update users 
		set users.activity = users.activity + 1 
	where users.id = NEW.user_id;
END - 
DELIMITER ;

-- аналогично можно еще на update и на delete и на другие таблицы.
-- тогда вышеуказанный запрос будет иметь вид:
SELECT 
	id,
	activity
FROM users
order by activity
LIMIT 10;

-- еще вариант - более общий:
-- создать таблицу user_log
create table user_log (
	id SERIAL PRIMARY KEY,
	user_id int unsigned,
	target_type_id int unsigned,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
-- создаем тригерры типа:
drop trigger if exists users_activity;
DELIMITER -
create trigger user_log_activity after insert on likes
for each row BEGIN
	insert into user_log (user_id, target_type_id) values  
		(new.user_id,
		(select id from target_types where name LIKE 'likes')
		);
END - 
DELIMITER ;
-- тогда вышеуказанный запрос будет иметь вид:
SELECT
	user_id,
	count(user_id) as qty
from user_log
group by user_id
order by qty
LIMIT 10;
-- кроме того здесь получается дополнительная возможность делать выборку за определенный период (беря во внимание колонку created_at), а также можем еще считать в разрезе таблиц, то есть в каких таблицах проходили активности (беря во внимание колонку target_type_id)