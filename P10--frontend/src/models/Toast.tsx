import {AlertColor} from "@mui/material/Alert/Alert";

export enum ToastActionTypes {
    ADD_TOAST,
    DELETE_TOAST
}

export type ToastActions =
    { type: ToastActionTypes.ADD_TOAST; toast: Toast }
    | { type: ToastActionTypes.DELETE_TOAST; id: string }

export type ToastState = {
    toasts: Toast[];
}

export interface Toast {
    id: string;
    type: AlertColor;
    header: string;
    message: string;
}
