package io.github.matian2014.candys3;

public enum ObjectRetentionMode {

    /**
     * In compliance mode, a protected object version can't be overwritten or deleted by any user, including the root user in your AWS account.
     * When an object is locked in compliance mode, its retention mode can't be changed, and its retention period can't be shortened.
     * Compliance mode helps ensure that an object version can't be overwritten or deleted for the duration of the retention period.
     * <p>
     * Note: The only way to delete an object under the compliance mode before its retention date expires is to delete the associated AWS account.
     */
    COMPLIANCE,

    /**
     * In governance mode, users can't overwrite or delete an object version or alter its lock settings unless they have special permissions.
     * With governance mode, you protect objects against being deleted by most users, but you can still grant some users permission to alter the retention settings or delete the objects if necessary.
     * You can also use governance mode to test retention-period settings before creating a compliance-mode retention period.
     */
    GOVERNANCE;
}
