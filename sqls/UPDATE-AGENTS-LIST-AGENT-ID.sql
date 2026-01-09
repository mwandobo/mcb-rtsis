UPDATE AGENTS_LIST a
SET agent_id =
        CHAR(
                61221428 +
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
