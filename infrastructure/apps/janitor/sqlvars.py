jira_inactive_users_query = """
SELECT
    d.directory_name AS "Directory",
    u.user_name AS "Username",
    to_timestamp(CAST(ca.attribute_value AS BIGINT) / 1000) AS "Last Login",
    mem.parent_name AS "License group"
FROM
    cwd_user u
    JOIN cwd_directory d ON u.directory_id = d.id
    LEFT JOIN cwd_user_attributes ca ON u.id = ca.user_id
    AND ca.attribute_name = 'login.lastLoginMillis'
    JOIN cwd_membership mem ON mem.lower_child_name = u.lower_user_name
    AND mem.lower_parent_name IN (
        SELECT
            group_id
        FROM
            licenserolesgroup
        WHERE
            license_role_name not in ('jira-core', 'jira-servicedesk')
            and group_id != 'jira-administrators'
    )
WHERE
    u.active = 1
    AND d.active = 1
    AND u.lower_user_name IN (
        SELECT
            lower_child_name
        FROM
            cwd_membership m
            JOIN licenserolesgroup gp ON m.lower_parent_name = lower(gp.GROUP_ID)
        WHERE
            m.lower_parent_name IN (
                SELECT
                    group_id
                FROM
                    licenserolesgroup
                WHERE
                    license_role_name not in ('jira-core', 'jira-servicedesk')
                    and group_id != 'jira-administrators'
            )
    )
    AND (
        u.id IN (
            SELECT
                ca.user_id
            FROM
                cwd_user_attributes ca
            WHERE
                attribute_name = 'login.lastLoginMillis'
                AND to_timestamp(CAST(ca.attribute_value as bigint) / 1000) <= current_date - INTERVAL '%s days'
        )
        OR u.id NOT IN (
            SELECT
                ca.user_id
            FROM
                cwd_user_attributes ca
            WHERE
                attribute_name = 'login.lastLoginMillis'
        )
    )
    AND u.lower_user_name NOT IN (
        SELECT
            lower_user_name
        from
            cwd_user
            join cwd_membership as cm on cm.lower_child_name = cwd_user.lower_user_name
        where
            cm.lower_parent_name IN ('script_users_jira')
    )
ORDER BY
    "Last Login" DESC;
"""

jsm_inactive_users_query = """
SELECT
    cu.lower_user_name as "User name",
    subci.created AS last_servicedesk_action,
    cm.parent_name AS "License group"
FROM
    cwd_user AS cu
    INNER JOIN cwd_membership AS cm ON cu.directory_id = cm.directory_id
    AND cu.lower_user_name = cm.lower_child_name
    AND cm.membership_type = 'GROUP_USER'
    AND cm.parent_name IN (
        SELECT
            group_id
        FROM
            licenserolesgroup lrg
        WHERE
            license_role_name = 'jira-servicedesk'
    )
    LEFT JOIN LATERAL (
        SELECT
            *
        from
            (
                SELECT
                    cg.author,
                    cg.created,
                    row_number() over(
                        partition by cg.author
                        order by
                            cg.created desc
                    ) as rn
                FROM
                    changegroup cg
                    join changeitem ci on ci.groupid = cg.id
                    join jiraissue issu on cg.issueid = issu.id
                    join project pr on issu.project = pr.id
                WHERE
                    cu.user_name = cg.author
                    AND pr.projecttype = 'service_desk'
            ) t
        where
            t.rn = 1
    ) subci ON subci.author = cu.user_name
    LEFT JOIN LATERAL (
        select
            *
        from
            (
                select
                    "SECONDARY_RESOURCE_ID",
                    "ACTION",
                    "SEARCH_STRING",
                    "ENTITY_TIMESTAMP",
                    row_number() over(
                        partition by audit."SECONDARY_RESOURCE_ID"
                        order by
                            audit."ENTITY_TIMESTAMP" desc
                    ) as rn
                FROM
                    "AO_C77861_AUDIT_ENTITY" AS audit
                WHERE
                    audit."SECONDARY_RESOURCE_ID" = cu.user_name
                    AND audit."ACTION" = 'User added to group'
                    AND audit."SEARCH_STRING" like CONCAT(cm.lower_parent_name, '%%')
            ) h
        WHERE
            h.rn = 1
    ) ae ON ae."SECONDARY_RESOURCE_ID" = cu.user_name
WHERE
    (
        (
            subci.created < current_date - INTERVAL '%s days'
            OR subci.created is null
        )
    )
    and cu.active = 1
    AND (
        to_timestamp(CAST(ae."ENTITY_TIMESTAMP" as bigint) / 1000) < current_date - INTERVAL '14 days'
    )
    AND cu.lower_user_name NOT IN (
        SELECT
            lower_user_name
        from
            cwd_user
            join cwd_membership as cm on cm.lower_child_name = cwd_user.lower_user_name
        where
            cm.lower_parent_name IN ('script_users_jsm')
    )
"""

confluence_inactive_users_query = """
SELECT
    u.user_name AS "Username",
    l.successdate AS "Last Login"
FROM
    logininfo l
    RIGHT JOIN user_mapping m ON m.user_key = l.username
    RIGHT JOIN cwd_user u on m.username = u.user_name
WHERE
    (
        successdate < CURRENT_DATE - INTERVAL '%s days'
        OR successdate IS NULL
    )
    AND u.active = 'T'
    AND u.user_name not IN (
        select
            u.user_name
        from
            cwd_user u full
            join cwd_membership m on u.id = child_user_id full
            join cwd_group g on m.parent_id = g.id
        WHERE
            g.group_name = 'confluence-administrators'
    )
    AND (
        u.user_name IN (
            SELECT
                u.user_name
            FROM
                cwd_user u,
                cwd_membership m,
                cwd_group g
            WHERE
                u.id = child_user_id
                AND m.parent_id = g.id
                AND g.group_name in ('confluence-users')
        )
    )
    AND u.lower_user_name NOT IN (
        SELECT
            lower_user_name
        from
            cwd_user
            join cwd_membership cm on cm.child_user_id = cwd_user.id
        where
            cm.parent_id IN (
                Select
                    id
                from
                    cwd_group
                where
                    group_name = 'script_users_confluence'
            )
    )
ORDER BY
    "Last Login" DESC;
"""

stash_inactive_users_query = """
SELECT
    cu.lower_user_name as "User name",
    to_timestamp(CAST(cua.attribute_value AS BIGINT) / 1000) AS last_login_browser,
    to_timestamp(CAST(subra.last_accessed AS bigint) / 1000) AS last_login_console,
    cm.parent_name AS "License group"
FROM
    cwd_user AS cu
    LEFT JOIN cwd_user_attribute AS cua ON cu.ID = cua.user_id
    AND cua.attribute_name = 'lastAuthenticationTimestamp'
    LEFT JOIN sta_normal_user AS snu ON snu.name = cu.user_name
    LEFT JOIN (
        SELECT
            *
        from
            (
                SELECT
                    user_id,
                    last_accessed,
                    row_number() over(
                        partition by user_id
                        order by
                            last_accessed desc
                    ) as rn
                FROM
                    repository_access
            ) t
        where
            t.rn = 1
    ) subra ON subra.user_id = snu.user_id
    INNER JOIN cwd_membership AS cm ON cu.directory_id = cm.directory_id
    AND cu.lower_user_name = cm.lower_child_name
    AND cm.membership_type = 'GROUP_USER'
    AND cm.lower_parent_name IN (
        SELECT
            group_name
        FROM
            sta_global_permission
        where
            group_name IS NOT null
            and group_name not in ('jira-administrators', 'stash-admins')
    )
WHERE
    (
        (
            to_timestamp(CAST(cua.attribute_value AS BIGINT) / 1000) < current_date - INTERVAL '%s DAYS'
            AND cua.attribute_value is null
        )
        OR (
            to_timestamp(CAST(cua.attribute_value AS BIGINT) / 1000) < current_date - INTERVAL '%s DAYS'
            AND to_timestamp(CAST(subra.last_accessed AS BIGINT) / 1000) < current_date - INTERVAL '%s DAYS'
        )
        OR (
            cua.attribute_value is null
            AND subra.last_accessed is null
        )
        OR (
            to_timestamp(CAST(subra.last_accessed AS BIGINT) / 1000) < current_date - INTERVAL '%s DAYS'
            AND subra.last_accessed is null
        )
    )
    and cu.is_active = 'T'
    AND cu.lower_user_name NOT IN (
        SELECT
            lower_user_name
        from
            cwd_user
            join cwd_membership as cm on cm.lower_child_name = cwd_user.lower_user_name
        where
            cm.lower_parent_name IN ('script_users_bitbucket')
    )
"""
stash_current_used_licences_query = """SELECT
    COUNT(*)
FROM
    "AO_A020FF_LICENSED_USER"
GROUP BY
    "VERSION"
ORDER BY
    1 DESC;"""

jira_audit_added_to_group_within_14_days = """
select
    count(*)
FROM
    "AO_C77861_AUDIT_ENTITY" AS audit
WHERE
    audit."ACTION" = 'User added to group'
    AND audit."SEARCH_STRING" like CONCAT('%s', ' ', '%s', '%%')
    AND (
        to_timestamp(CAST(audit."ENTITY_TIMESTAMP" as bigint) / 1000) > current_date - INTERVAL '14 days'
    )
"""

jira_current_used_licences_query = """
SELECT
    DISTINCT COUNT(u.lower_user_name)
FROM
    cwd_user u
    JOIN cwd_membership m ON u.id = m.child_id
    AND u.directory_id = m.directory_id
    JOIN licenserolesgroup lrg ON Lower(m.parent_name) = Lower(lrg.group_id)
    JOIN cwd_directory d ON m.directory_id = d.id
WHERE
    d.active = '1'
    AND u.active = '1'
    AND license_role_name = 'jira-software';
"""
jsm_current_used_licences_query = """
SELECT
    DISTINCT COUNT(u.lower_user_name)
FROM
    cwd_user u
    JOIN cwd_membership m ON u.id = m.child_id
    AND u.directory_id = m.directory_id
    JOIN licenserolesgroup lrg ON Lower(m.parent_name) = Lower(lrg.group_id)
    JOIN cwd_directory d ON m.directory_id = d.id
WHERE
    d.active = '1'
    AND u.active = '1'
    AND license_role_name = 'jira-servicedesk';
"""
confluence_current_used_licences_query = """
SELECT
    DISTINCT COUNT(u.lower_user_name)
FROM
    cwd_user u
    JOIN cwd_membership m ON u.id = child_user_id
    JOIN cwd_group g ON m.parent_id = g.id
    JOIN SPACEPERMISSIONS sp ON g.group_name = sp.PERMGROUPNAME
    JOIN cwd_directory d on u.directory_id = d.id
WHERE
    sp.PERMTYPE = 'USECONFLUENCE'
    AND u.active = 'T'
    AND d.active = 'T'
"""
