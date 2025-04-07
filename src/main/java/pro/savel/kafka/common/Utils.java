package pro.savel.kafka.common;

public abstract class Utils {

    public static String combineErrorMessage(Throwable exception) {
        var builder = new StringBuilder(exception.getMessage().length());
        builder.append(exception.getMessage());
        while (true) {
            exception = exception.getCause();
            if (exception == null)
                break;
            builder.append("\n");
            builder.append(exception.getMessage());
        }
        return builder.toString();
    }
}
