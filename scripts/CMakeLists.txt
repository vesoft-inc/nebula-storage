install(
    FILES
        nebula-storaged.service
        nebula-metad.service
        nebula.service
        services.sh
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
)

install(
    FILES
        utils.sh
        meta.hosts
        storage.hosts
    PERMISSIONS
        OWNER_WRITE OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        scripts
)
