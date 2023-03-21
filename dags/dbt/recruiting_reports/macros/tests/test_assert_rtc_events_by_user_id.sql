{% macro test_assert_rtc_events_by_user_id(model=None, column_name=None) %}

    {% if target.name == 'STAGE' %}
        {% set users_events = [
            {
                "id": 347266,
                "test_start_date": '2020-01-01',
                "test_end_date": '2022-01-02',
                "project_rejected": 0,
                "project_revoked": 0,
                "project_activation": 4,
                "user_revoked": 0,
                "user_activation_after_proj_active": 0
            }, 
            {
                "id": 317113,
                "test_start_date": '2020-01-01',
                "test_end_date": '2022-01-02',
                "project_rejected": 0,
                "project_revoked": 0,
                "project_activation": 3,
                "user_revoked": 0,
                "user_activation_after_proj_active": 0
            },
        ] %}
    {% elif target.name == 'PROD' %}
        {% set users_events = [
            {
                "id": 4765605,
                "test_start_date": '2019-01-01',
                "test_end_date": '2022-01-01',
                "project_rejected": 0,
                "project_revoked": 0,
                "project_activation": 0,
                "user_revoked": 0,
                "user_activation_after_proj_active": 0
            },
            {
                "id": 317368,
                "test_start_date": '2019-01-01',
                "test_end_date": '2022-01-01',
                "project_rejected": 0,
                "project_revoked": 1,
                "project_activation": 0,
                "user_revoked": 0,
                "user_activation_after_proj_active": 1
            },
            {
                "id": 2163389,
                "test_start_date": '2019-01-01',
                "test_end_date": '2022-01-01',
                "project_rejected": 0,
                "project_revoked": 3,
                "project_activation": 3,
                "user_revoked": 0,
                "user_activation_after_proj_active": 1
            },
            {
                "id": 2001167,
                "test_start_date": '2019-01-01',
                "test_end_date": '2020-01-01',
                "project_rejected": 0,
                "project_revoked": 0,
                "project_activation": 1,
                "user_revoked": 2,
                "user_activation_after_proj_active": 1
            },
            {
                "id": 1693383,
                "test_start_date": '2019-01-01',
                "test_end_date": '2022-01-01',
                "project_rejected": 0,
                "project_revoked": 0,
                "project_activation": 2,
                "user_revoked": 2,
                "user_activation_after_proj_active": 0
            },
            {
                "id": 423824,
                "test_start_date": '2012-01-01',
                "test_end_date": '2022-01-01',
                "project_rejected": 0,
                "project_revoked": 3,
                "project_activation": 7,
                "user_revoked": 1,
                "user_activation_after_proj_active": 1
            },
            {
                "id": 5759943,
                "test_start_date": '2021-01-01',
                "test_end_date": '2022-10-10',
                "project_rejected": 4,
                "project_revoked": 7,
                "project_activation": 24,
                "user_revoked": 1,
                "user_activation_after_proj_active": 4
            },
            {
                "id": 1497953,
                "test_start_date": '2021-01-01',
                "test_end_date": '2022-09-15',
                "project_rejected": 0,
                "project_revoked": 4,
                "project_activation": 3,
                "user_revoked": 0,
                "user_activation_after_proj_active": 1
            },
            {
                "id": 5628480,
                "test_start_date": '2022-01-01',
                "test_end_date": '2022-10-10',
                "project_rejected": 0,
                "project_revoked": 3,
                "project_activation": 3,
                "user_revoked": 0,
                "user_activation_after_proj_active": 1
            },
            {
                "id": 6019300,
                "test_start_date": '2021-01-01',
                "test_end_date": '2022-09-30',
                "project_rejected": 0,
                "project_revoked": 1,
                "project_activation": 1,
                "user_revoked": 0,
                "user_activation_after_proj_active": 2
            },
            {
                "id": 1597731,
                "test_start_date": '2019-01-01',
                "test_end_date": '2022-08-01',
                "project_rejected": 1,
                "project_revoked": 16,
                "project_activation": 34,
                "user_revoked": 0,
                "user_activation_after_proj_active": 0
            },
            {
                "id": 7886207,
                "test_start_date": '2022-01-01',
                "test_end_date": '2022-09-01',
                "project_rejected": 0,
                "project_revoked": 0,
                "project_activation": 0,
                "user_revoked": 0,
                "user_activation_after_proj_active": 1
            },
            {
                "id": 1695183,
                "test_start_date": '2019-01-01',
                "test_end_date": '2021-01-01',
                "project_rejected": 0,
                "project_revoked": 0,
                "project_activation": 0,
                "user_revoked": 1,
                "user_activation_after_proj_active": 1
            },
        ] %}
    {% endif %}

    SELECT
        user_id
        , action_type
        , COUNT(action_type) AS event_count
    FROM {{ ref('new_rtc_and_revoked_users' ) }}
    WHERE
        {% for user in users_events %}
            {% if loop.first %}
                ( user_id = '{{ user['id'] | as_number }}'
                    AND date_created > '{{ user['test_start_date'] }}'
                    AND date_created < '{{ user['test_end_date'] }}'
                )
            {% else %}
                OR ( user_id = '{{ user['id'] | as_number }}'
                    AND date_created > '{{ user['test_start_date'] }}'
                    AND date_created < '{{ user['test_end_date'] }}'
                )
            {% endif %}
        {% endfor %}
    GROUP BY 1, 2
    HAVING
        {% for user in users_events %}
            {% if loop.first %}
                (user_id = '{{ user['id'] | as_number }}'
                    AND(
                        (action_type = 'project_activation'
                            AND event_count != '{{ user['project_activation'] | as_number }}'
                        )
                        OR(action_type = 'project_revoked'
                            AND event_count != '{{ user['project_revoked'] | as_number }}'
                        )
                        OR(action_type = 'user_revoked'
                            AND event_count != '{{ user['user_revoked'] | as_number }}'
                        )
                        OR(action_type = 'user_activation_after_proj_active'
                            AND event_count != '{{ user['user_activation_after_proj_active'] | as_number }}'
                        )
                        OR(action_type = 'project_rejected'
                            AND event_count != '{{ user['project_rejected'] | as_number }}'
                        )
                    )
                )
            {% else %}
                OR ( user_id = '{{ user['id'] | as_number }}'
                    AND(
                        (action_type = 'project_activation'
                            AND event_count != '{{ user['project_activation'] | as_number }}'
                        )
                        OR(action_type = 'project_revoked'
                            AND event_count != '{{ user['project_revoked'] | as_number }}'
                        )
                        OR(action_type = 'user_revoked'
                            AND event_count != '{{ user['user_revoked'] | as_number }}'
                        )
                        OR(action_type = 'user_activation_after_proj_active'
                            AND event_count != '{{ user['user_activation_after_proj_active'] | as_number }}'
                        )
                        OR(action_type = 'project_rejected'
                            AND event_count != '{{ user['project_rejected'] | as_number }}'
                        )
                    )
                )
            {% endif %}
        {% endfor %}
{% endmacro %}
