package pro.savel.kafka.admin;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.kafka.clients.admin.Admin;
import pro.savel.kafka.common.ClientWrapper;

import java.util.Properties;

@Getter
@EqualsAndHashCode(callSuper = false)
public class AdminWrapper extends ClientWrapper {

    private final Admin admin;

    protected AdminWrapper(String name, Properties config, int expirationTimeout) {
        super(name, config, expirationTimeout);
        admin = Admin.create(config);
    }

    @Override
    public void close() {
        admin.close();
    }
}
