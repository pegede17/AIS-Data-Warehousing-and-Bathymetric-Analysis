import {ToastActionTypes} from "../models/Toast";
import {useContext} from "react";
import {ToastStateContext} from "../contexts/toastContext";
import {AlertColor} from "@mui/material/Alert/Alert";

export function useToast() {
    const {dispatch} = useContext(ToastStateContext);
    const delay = 2500; // Total ms before a toast is removed

    function toast(type: AlertColor, message: string, header: string) {
        const id = Math.random().toString(36).slice(2, 9); // Random generated ID

        dispatch({
            type: ToastActionTypes.ADD_TOAST,
            toast: {
                type, message, header, id
            }
        });

        setTimeout(() => {
            dispatch({
                type: ToastActionTypes.DELETE_TOAST,
                id
            })
        }, delay);
    }

    return toast;
}
