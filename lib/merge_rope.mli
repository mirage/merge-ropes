
(*
 * Copyright (c) 2013-2014 Thomas Gazagnaire <thomas@gazagnaire.org>
 * Copyright (c) 2015 KC Sivaramakrishnan <sk826@cl.cam.ac.uk>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*)

type stat = {
  ops : int;
  reads : int;
  writes : int;
}

type stats = {
  set : (int * stat) list;
  get : (int * stat) list;
  insert : (int * stat) list;
  delete : (int * stat) list;
  append : (int * stat) list;
  split : (int * stat) list;
  merge : (int * stat) list;
}

val string_of_stat: stat -> string
val string_of_statlist: (int * stat) list -> string

module type S = sig

  include Irmin.Contents.S
  type value
  type cont

  val create : unit -> t Lwt.t
  (* Create a new empty rope. *)
  val make : cont -> t Lwt.t
  (* Construct a new rope from the container [cont] *)
  val flush : t -> cont Lwt.t
  (* Return the container represented by the rope [t] *)

  val is_empty : t -> bool Lwt.t
  (* Return true if the rope [t] is empty, false otherwise. *)
  val length : t -> int Lwt.t
  (* Return the length of the rope [t]. *)

  val set : t -> pos:int -> value -> t Lwt.t
  (* Set the value at the position [pos] in the rope [t] to [value]. Complexity: log(n) *)
  val get : t -> pos:int -> value Lwt.t
  (* Get the value containing in the rope [t] at the position [pos]. Complexity: log(n) *)
  val insert : t -> pos:int -> cont -> t Lwt.t
  (* Insert the container [cont] in the rope [t] at the position [pos]. Complexity: log(n) *)
  val delete : t -> pos:int -> len:int -> t Lwt.t
  (* Delete [len] elements after the position [pos] in the rope [t]. Complexity: log(n) *)
  val append : t -> t -> t Lwt.t
  (* Append the two given ropes. Complexity: log(n) *)
  val concat : sep:t -> t list -> t Lwt.t
  (* Concatenate the ropes containing in [list] with the separator [sep]. Complexity: Sum (log n) *)
  val split : t -> pos:int -> (t * t) Lwt.t
  (* Split the rope [t] at the position [pos] into two ropes. Complexity: log(n) *)

  val stats : unit -> stats
  (* Return statistics about the rope. *)
  val reset : unit -> unit
  (* Reset statistic internal counters. *)
end

module type Config = sig
  val conf: Irmin.config
  val task: string -> Irmin.task
end

module Make
    (AO: Irmin.AO_MAKER)
    (K: Irmin.Hash.S)
    (V:
     sig
       type a
       include Irmin.Contents.S

       val empty : t
       val length : t -> int
       val set : t -> int -> a -> t
       val get : t -> int -> a
       val insert : t -> int -> t -> t
       val delete : t -> int -> int -> t
       val append : t -> t -> t
       val concat : t -> t list -> t
       val split : t -> int -> (t * t)
     end)
    (C: Config)
  : S with type value = V.a
       and type cont = V.t
       and module Path = V.Path
