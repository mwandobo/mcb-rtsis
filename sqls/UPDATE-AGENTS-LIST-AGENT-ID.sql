UPDATE AGENTS_LIST a
SET agent_id =
        CHAR(
                (
                    SELECT MAX(INTEGER(TRIM(AGENT_ID)))
                    FROM AGENTS_LIST
                    WHERE AGENT_ID IS NOT NULL
                      AND TRIM(AGENT_ID) <> ''
                      AND TRANSLATE(AGENT_ID, '', '0123456789') = ''
                )
                    +
                (
                    SELECT COUNT(*)
                    FROM AGENTS_LIST b
                    WHERE
                        (
                            b.agent_id IS NULL
                                OR LENGTH(b.agent_id) = 0
                                OR TRANSLATE(b.agent_id, '', '0123456789') <> ''
                                OR b.agent_id = '0'
                            )
                      AND b.terminal_id <= a.terminal_id
                )
        )
WHERE
    a.agent_id IS NULL
   OR LENGTH(a.agent_id) = 0
   OR TRANSLATE(a.agent_id, '', '0123456789') <> ''
   OR a.agent_id = '0';
