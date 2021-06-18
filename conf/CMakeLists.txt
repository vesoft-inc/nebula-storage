# These configuration files are for reference to generate your own customized ones.
# Thus, they are installed as read-only, even for the owner.
install(
    FILES
        nebula-storaged.conf.default
        nebula-storaged.conf.production
        nebula-storaged-listener.conf.production
    PERMISSIONS
        OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        etc
    COMPONENT
        storage
)

install(
    FILES
        nebula-metad.conf.default
        nebula-metad.conf.production
    PERMISSIONS
        OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        etc
    COMPONENT
        meta
)
