package io.spring2go.seata.common.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Created by william on May, 2020
 */
public class StringUtils {
    private StringUtils() {

    }

    public static final boolean isEmpty(String str) {
        return (str == null) || (str.isEmpty());
    }

    public static Blob string2blob(String str) throws SQLException {
        return BlobUtils.string2blob(str);
    }

    public static String blob2string(Blob blob) throws SQLException {
        return BlobUtils.blob2string(blob);
    }

    public static String inputStream2String(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int i = -1;
        while ((i = is.read()) != -1) {
            baos.write(i);
        }
        return baos.toString();
    }
}
